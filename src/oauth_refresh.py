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

Multi-account mode: each account has its own .credentials.json living
under its own /data/shared-auth/<account>/ directory. The refresher
walks every configured path each tick; rotations are independent per
account, but the single-writer invariant still holds because no other
process in the deployment writes any of those files."""
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
    """Watches one or more credentials.json files and refreshes each
    account's OAuth token before it expires. Designed to be the only
    place in the deployment that talks to /v1/oauth/token — keeps
    refresh_token rotation single-writer per account.

    Each account is treated independently: a slow / failed refresh on
    one does not block the loop for the others. Per-account refresh
    happens serially within a tick (we do not parallelize the HTTP POST
    across accounts) because that codepath is rare (every ~check_interval
    AND token-expiring-within-window) and serial is simpler to reason
    about — there's no contention to win by parallelizing."""

    def __init__(
        self,
        credentials_paths: list[Path],
        check_interval_seconds: float = 300.0,
        refresh_when_expires_within_seconds: float = 3600.0,
    ) -> None:
        if not credentials_paths:
            raise ValueError("OAuthRefresher needs at least one credentials path")
        self.credentials_paths = list(credentials_paths)
        self.check_interval = check_interval_seconds
        self.refresh_window = refresh_when_expires_within_seconds

    async def refresh_now(self) -> dict[str, str]:
        """Force one immediate refresh attempt across all accounts,
        bypassing the periodic loop's sleep. Returns a {path: status}
        map where status is one of:
          "refreshed"  — hit /v1/oauth/token and got a new token
          "not_needed" — token has more life than refresh_window
          "failed"     — any error (logged; refresh loop will retry)
        Used by POST /admin/refresh-now."""
        out: dict[str, str] = {}
        for path in self.credentials_paths:
            out[str(path)] = await self._refresh_path_now(path)
        return out

    async def _refresh_path_now(self, path: Path) -> str:
        creds = self._read_credentials(path)
        if creds is None:
            return "failed"
        oauth = creds.get("claudeAiOauth")
        if not isinstance(oauth, dict):
            return "failed"
        expires_at_ms = oauth.get("expiresAt")
        if not isinstance(expires_at_ms, (int, float)):
            return "failed"
        remaining = expires_at_ms / 1000.0 - time.time()
        rt = oauth.get("refreshToken")
        if not isinstance(rt, str) or not rt:
            return "failed"
        new_fields = await self._refresh(rt)
        if new_fields is None:
            return "failed"
        oauth.update(new_fields)
        creds["claudeAiOauth"] = oauth
        self._atomic_write(path, creds)
        log.info("[%s] forced refresh: token had %.0fs left; new expiresAt=%d",
                 path, remaining, oauth["expiresAt"])
        return "refreshed"

    async def initial_refresh(self) -> None:
        """One-shot pre-startup check across all accounts. Call before
        spawning any worker so that workers' first read of
        .credentials.json picks up a fresh token if the deployment had
        been sitting on a stale one. Errors are logged but never raised
        — the periodic loop will retry, and we don't want a refresh
        hiccup to block service startup."""
        try:
            await self._tick()
        except Exception:
            log.exception("initial oauth refresh check failed; "
                          "periodic loop will retry")

    async def run(self) -> None:
        """Periodic check loop. Designed to run as a background asyncio
        task for the lifetime of the FastAPI app. Catches and logs all
        per-tick errors so the loop never dies."""
        log.info("oauth refresher started accounts=%d "
                 "check_interval=%.0fs refresh_when_expires_within=%.0fs",
                 len(self.credentials_paths), self.check_interval,
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
        for path in self.credentials_paths:
            try:
                await self._tick_path(path)
            except Exception:
                log.exception("oauth refresher tick failed for %s; "
                              "other accounts continue", path)

    async def _tick_path(self, path: Path) -> None:
        creds = self._read_credentials(path)
        if creds is None:
            return
        oauth = creds.get("claudeAiOauth")
        if not isinstance(oauth, dict):
            log.warning("[%s] credentials file missing claudeAiOauth block; skip",
                        path)
            return
        expires_at_ms = oauth.get("expiresAt")
        if not isinstance(expires_at_ms, (int, float)):
            log.warning("[%s] credentials file missing/invalid expiresAt; skip",
                        path)
            return
        remaining = expires_at_ms / 1000.0 - time.time()
        if remaining > self.refresh_window:
            return  # plenty of life left
        rt = oauth.get("refreshToken")
        if not isinstance(rt, str) or not rt:
            log.warning("[%s] credentials file missing refreshToken; "
                        "cannot refresh", path)
            return

        log.info("[%s] token expires in %.0fs (<%.0fs threshold); refreshing",
                 path, remaining, self.refresh_window)
        new_fields = await self._refresh(rt)
        if new_fields is None:
            return  # error already logged; loop will retry next tick

        # Preserve scopes/subscriptionType/rateLimitTier (the refresh
        # response doesn't echo those back, and they don't change across
        # refreshes of the same RT chain).
        oauth.update(new_fields)
        creds["claudeAiOauth"] = oauth
        self._atomic_write(path, creds)
        new_remaining = oauth["expiresAt"] / 1000.0 - time.time()
        log.info("[%s] token refreshed; new expiresAt=%d (in %.0fs)",
                 path, oauth["expiresAt"], new_remaining)

    def _read_credentials(self, path: Path) -> dict[str, Any] | None:
        try:
            return json.loads(path.read_text())
        except FileNotFoundError:
            log.warning("credentials file not found at %s", path)
            return None
        except json.JSONDecodeError:
            log.warning("[%s] credentials file is not valid JSON; skip", path)
            return None
        except PermissionError as e:
            # Loud about this one because the silent failure mode is
            # particularly nasty: workers boot, claude CLI reads them
            # as "Not logged in", prewarm times out, account gets
            # marked rate-limited — operator chases a phantom quota
            # issue instead of running one chown.
            try:
                st = path.stat()
                owner_uid = st.st_uid
                file_mode = oct(st.st_mode & 0o777)
            except OSError:
                owner_uid = "?"
                file_mode = "?"
            log.error(
                "[%s] CANNOT READ credentials file (uid=%s mode=%s): %s. "
                "Container runs as uid 1000 — host file must be readable "
                "by that uid. Fix on the host:\n"
                "    sudo chown -R 1000:1000 /data/shared-auth",
                path, owner_uid, file_mode, e)
            return None
        except OSError as e:
            log.warning("[%s] credentials file read error: %s", path, e)
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
            # Body may contain raw tokens or error details that echo the
            # refresh_token; don't write any of it to log. Status + a
            # hint of the error type from JSON (if parseable) is enough
            # to debug refresh failures.
            err_type = None
            try:
                err_type = (r.json() or {}).get("error")
            except ValueError:
                pass
            log.error("refresh failed status=%d error=%s",
                      r.status_code, err_type)
            return None
        try:
            data = r.json()
        except ValueError:
            log.error("refresh response not JSON (status=%d, content_type=%s)",
                      r.status_code, r.headers.get("content-type"))
            return None

        new_at = data.get("access_token")
        # Some OAuth servers omit refresh_token from the response when
        # they don't rotate it. Fall back to the one we sent in.
        new_rt = data.get("refresh_token") or refresh_token
        expires_in = data.get("expires_in")
        if not isinstance(new_at, str) or not isinstance(expires_in, (int, float)):
            # Log which fields are present (not their values) so we can
            # still diagnose schema mismatches without writing a token.
            log.error("refresh response missing access_token or expires_in "
                      "(keys=%s)", sorted(data.keys()) if isinstance(data, dict) else type(data).__name__)
            return None
        return {
            "accessToken": new_at,
            "refreshToken": new_rt,
            "expiresAt": int((time.time() + expires_in) * 1000),
        }

    def _atomic_write(self, path: Path, creds: dict[str, Any]) -> None:
        """Mirror claude CLI's own write pattern (write tmp + rename) so
        a worker reading .credentials.json mid-refresh sees either the
        old or the new file, never a partial. Tempfile lives in the
        same directory so the rename is on the same filesystem (cross-
        device rename would error)."""
        d = path.parent
        fd, tmp = tempfile.mkstemp(prefix=".credentials.", suffix=".tmp",
                                   dir=str(d))
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(creds, f)
            # Match the 0600 perms on the original .credentials.json.
            os.chmod(tmp, 0o600)
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
