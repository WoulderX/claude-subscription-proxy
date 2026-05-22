"""Proactive OAuth token refresh, running once in the main process.

claude code CLI's normal flow is to refresh tokens lazily inside each
worker when its in-memory access_token approaches expiry. With multiple
workers sharing one OAuth account via the .claude/ symlink (our setup),
that produces a race:

  T0: workers A and B both cache {AT, RT} in process memory at startup
  T1: AT nears expiry; both workers concurrently POST /v1/oauth/token
      with the cached RT
  T2: one wins (gets new {AT', RT'}, atomic-writes .credentials.json);
      the other gets invalid_grant because RT was rotated and is now
      single-use-consumed
  T3: the loser keeps RT in memory forever, can never refresh, every
      request comes back 401 until the worker is restarted

To eliminate the race the main process becomes the SOLE refresher.
Workers never refresh on their own because the manager's scheduled
restart interval is kept comfortably shorter than the original token
lifetime — workers always get recycled (and re-read the shared
.credentials.json from disk) before their in-memory AT would expire.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Extracted from the bundled claude code CLI binary (Bun ELF,
# ~May 2026 build) via:
#   grep -aoE 'BASE_API_URL:"https://[^"]+"' claude.exe   -> api.anthropic.com
#   grep -aoE '"/v1/oauth/token"' claude.exe              -> token path
#   grep -aoE '"[0-9a-f-]{36}"' claude.exe                -> claude code OAuth client_id
# These are not secrets — claude code is distributed unobfuscated and
# the OAuth client is a public client (no client_secret). Only the
# per-user refresh_token grants account access.
TOKEN_URL = "https://api.anthropic.com/v1/oauth/token"
CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"


class OAuthRefresher:
    """Watches a credentials.json file and refreshes the OAuth token
    before it expires. Designed to be the only place in the deployment
    that talks to /v1/oauth/token — keeps refresh_token rotation
    single-writer."""

    def __init__(
        self,
        credentials_path: Path,
        check_interval_seconds: float = 300.0,
        refresh_when_expires_within_seconds: float = 3600.0,
    ) -> None:
        self.credentials_path = credentials_path
        self.check_interval = check_interval_seconds
        self.refresh_window = refresh_when_expires_within_seconds

    async def initial_refresh(self) -> None:
        """One-shot pre-startup check. Call before spawning any worker so
        that workers' first read of .credentials.json picks up a fresh
        token if the deployment had been sitting on a stale one. Errors
        are logged but never raised — the periodic loop will retry, and
        we don't want a refresh hiccup to block service startup."""
        try:
            await self._tick()
        except Exception:
            log.exception("initial oauth refresh check failed; "
                          "periodic loop will retry")

    async def run(self) -> None:
        """Periodic check loop. Designed to run as a background asyncio
        task for the lifetime of the FastAPI app. Catches and logs all
        per-tick errors so the loop never dies."""
        log.info("oauth refresher started credentials=%s "
                 "check_interval=%.0fs refresh_when_expires_within=%.0fs",
                 self.credentials_path, self.check_interval,
                 self.refresh_window)
        while True:
            try:
                await asyncio.sleep(self.check_interval)
                await self._tick()
            except asyncio.CancelledError:
                log.info("oauth refresher stopping")
                return
            except Exception:
                log.exception("oauth refresher tick failed; "
                              "will retry next interval")

    async def _tick(self) -> None:
        creds = self._read_credentials()
        if creds is None:
            return
        oauth = creds.get("claudeAiOauth")
        if not isinstance(oauth, dict):
            log.warning("credentials file missing claudeAiOauth block; skip")
            return
        expires_at_ms = oauth.get("expiresAt")
        if not isinstance(expires_at_ms, (int, float)):
            log.warning("credentials file missing/invalid expiresAt; skip")
            return
        remaining = expires_at_ms / 1000.0 - time.time()
        if remaining > self.refresh_window:
            return  # plenty of life left
        rt = oauth.get("refreshToken")
        if not isinstance(rt, str) or not rt:
            log.warning("credentials file missing refreshToken; cannot refresh")
            return

        log.info("token expires in %.0fs (<%.0fs threshold); refreshing",
                 remaining, self.refresh_window)
        new_fields = await self._refresh(rt)
        if new_fields is None:
            return  # error already logged; loop will retry next tick

        # Preserve scopes/subscriptionType/rateLimitTier (the refresh
        # response doesn't echo those back, and they don't change across
        # refreshes of the same RT chain).
        oauth.update(new_fields)
        creds["claudeAiOauth"] = oauth
        self._atomic_write(creds)
        new_remaining = oauth["expiresAt"] / 1000.0 - time.time()
        log.info("token refreshed; new expiresAt=%d (in %.0fs)",
                 oauth["expiresAt"], new_remaining)

    def _read_credentials(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.credentials_path.read_text())
        except FileNotFoundError:
            log.warning("credentials file not found at %s", self.credentials_path)
            return None
        except json.JSONDecodeError:
            log.warning("credentials file is not valid JSON; skip")
            return None
        except OSError as e:
            log.warning("credentials file read error: %s", e)
            return None

    async def _refresh(self, refresh_token: str) -> dict[str, Any] | None:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
        }
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(TOKEN_URL, json=payload)
        except httpx.HTTPError as e:
            log.warning("refresh network error: %s; will retry", e)
            return None
        if r.status_code != 200:
            log.error("refresh failed status=%d body=%s",
                      r.status_code, r.text[:300])
            return None
        try:
            data = r.json()
        except ValueError:
            log.error("refresh response not JSON: %s", r.text[:300])
            return None

        new_at = data.get("access_token")
        # Some OAuth servers omit refresh_token from the response when
        # they don't rotate it. Fall back to the one we sent in.
        new_rt = data.get("refresh_token") or refresh_token
        expires_in = data.get("expires_in")
        if not isinstance(new_at, str) or not isinstance(expires_in, (int, float)):
            log.error("refresh response missing access_token or expires_in: %s",
                      str(data)[:300])
            return None
        return {
            "accessToken": new_at,
            "refreshToken": new_rt,
            "expiresAt": int((time.time() + expires_in) * 1000),
        }

    def _atomic_write(self, creds: dict[str, Any]) -> None:
        """Mirror claude CLI's own write pattern (write tmp + rename) so
        a worker reading .credentials.json mid-refresh sees either the
        old or the new file, never a partial. Tempfile lives in the
        same directory so the rename is on the same filesystem (cross-
        device rename would error)."""
        d = self.credentials_path.parent
        fd, tmp = tempfile.mkstemp(prefix=".credentials.", suffix=".tmp",
                                   dir=str(d))
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(creds, f)
            # Match the 0600 perms on the original .credentials.json.
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.credentials_path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
