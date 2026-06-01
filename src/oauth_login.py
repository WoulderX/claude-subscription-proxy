"""Dashboard-driven `claude auth login` flow.

A LoginSession spawns the upstream `claude auth login --claudeai` CLI
under a PTY in a fresh tempdir HOME, captures the OAuth authorize URL
it prints, and waits for an operator-supplied callback code to feed
back into stdin. When the CLI exits, the freshly-written
`.credentials.json` + `.claude.json` are moved into the assigned
`/data/shared-auth/{name}/` directory and the manager spawns workers.

Why go through the official CLI instead of mirroring its OAuth wire
protocol ourselves: the CLI version is pinned in the Dockerfile
(CLAUDE_CODE_VERSION=2.1.139), and pin bumps are explicit human
decisions — re-validating this flow against the new CLI is a much
smaller maintenance surface than chasing the undocumented
client_id / redirect_uri / token_endpoint / user_info quartet ourselves.

External actors:
  - `POST /admin/accounts/new`         → LoginSession.begin (returns URL)
  - `POST /admin/accounts/new/{name}/finish` → LoginSession.finish (writes code, awaits exit, moves files)
  - automatic timeout sweeper          → drops abandoned sessions after FLOW_LIFETIME_SECONDS

Concurrency: at most one LoginSession per account name. Multiple
account names can be in-flight in parallel (different ports/tempdirs)
but practically the dashboard only drives one at a time."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any


def _ensure_onboarding_complete(path: Path) -> None:
    """Patch a freshly-written .claude.json so claude CLI doesn't drop
    a new worker onto the "Select login method" first-run picker.

    `claude auth login --claudeai` writes everything needed for token
    refresh (oauthAccount, userID, expiresAt, accountUuid, ...) but
    leaves hasCompletedOnboarding out — claude CLI then prompts at
    next launch. Setting the flag here is enough; the entrypoint stub
    has been using the same single-key stub for years."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    if data.get("hasCompletedOnboarding") is True:
        return
    data["hasCompletedOnboarding"] = True
    # Atomic write so a concurrent worker boot reading the same file
    # never observes a half-written JSON.
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

import ptyprocess

log = logging.getLogger(__name__)


# How long an in-progress login can sit waiting for the operator to
# paste the code. The CLI itself has no timeout; the sweeper enforces
# this to avoid orphaned PTY processes + tempdirs piling up.
FLOW_LIFETIME_SECONDS = 600.0

# After writing the code, how long we wait for `claude auth login` to
# exit (token exchange + .credentials.json write). On a healthy network
# this takes <2s; cap at 30s to fail fast on hangs.
FINISH_TIMEOUT_SECONDS = 30.0

# How long to wait for the CLI to print the authorize URL after spawn.
# The CLI also fetches MCP-registry + bootstrap before showing the URL,
# so 15s is generous; tighten if cold startup is fast in practice.
BEGIN_URL_TIMEOUT_SECONDS = 20.0


# Anthropic's authorize URL has a fixed prefix; we accept either
# `claude.com/cai/oauth/authorize` (2.1.139+) or `claude.ai/oauth/...`
# (older CLIs) in case the pin is bumped to a version that changed
# domains. The CLI prints the full URL on a single line preceded by
# "visit: ".
_URL_RE = re.compile(
    rb"https://(?:claude\.com/cai|claude\.ai)/oauth/authorize\?[^\s]+"
)
# The interactive prompt the CLI prints when it's waiting for the code.
# Used as a secondary "URL is now printed" signal — most CLI versions
# print the URL and the prompt in the same flush, but we also key off
# whichever lands first.
_PROMPT_HINT = b"Paste code"


class LoginSession:
    """One in-progress `claude auth login` flow."""

    def __init__(self, account_name: str, dest_dir: Path,
                 claude_binary: str = "claude",
                 pool: str | None = None,
                 priority: int | None = None) -> None:
        self.account_name = account_name
        self.dest_dir = dest_dir
        self.claude_binary = claude_binary
        # Operator-selected front-door sk-key. Persisted through to the
        # finish endpoint so dashboard-added accounts land in the
        # operator's chosen pool (sk-internal, sk-dev, …) rather than
        # always defaulting to api_key.
        self.pool = pool
        # Operator-selected routing tier (lower = preferred). None →
        # AccountConfig's default (100). Persisted same way as `pool`,
        # so the dashboard's add-account flow can drop a new account
        # straight into the Pro tier under the same front-door key.
        self.priority = priority
        # tempfile.mkdtemp owns the dir; cleanup happens in finish/abort.
        # Prefix makes orphan dirs easy to spot if something crashes
        # between mkdtemp and cleanup.
        self.tmp_home = Path(tempfile.mkdtemp(prefix=f"claude-login-{account_name}-"))
        self.proc: ptyprocess.PtyProcess | None = None
        self.authorize_url: str | None = None
        self.created_mono: float = time.monotonic()
        # PTY output accumulator — used to scan for the URL and the
        # prompt, also dumped on error for diagnostics.
        self._buf = bytearray()

    def is_expired(self) -> bool:
        return (time.monotonic() - self.created_mono) > FLOW_LIFETIME_SECONDS

    async def begin(self) -> str:
        """Spawn `claude auth login --claudeai` and read until the
        authorize URL is on screen. Returns the URL; caller serves it
        to the dashboard so the operator can open it in a browser."""
        env = os.environ.copy()
        env["HOME"] = str(self.tmp_home)
        env["TERM"] = "xterm-256color"
        # Same auto-update kill-switch as the worker PTY (see pty_driver.py).
        # The login flow runs for ~30s; an auto-update kicking in mid-flow
        # would race with the operator pasting the OAuth code.
        env["DISABLE_AUTOUPDATER"] = "1"
        # Strip any inherited claude-code marker env vars so the spawned
        # CLI doesn't think it's being launched by another claude.
        for k in ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID",
                  "CLAUDE_CODE_ENTRYPOINT", "AI_AGENT"):
            env.pop(k, None)
        try:
            self.proc = ptyprocess.PtyProcess.spawn(
                [self.claude_binary, "auth", "login", "--claudeai"],
                env=env, cwd=str(self.tmp_home),
                dimensions=(40, 120),
            )
        except Exception as e:
            self._cleanup()
            raise RuntimeError(f"failed to spawn claude auth login: {e}")

        loop = asyncio.get_running_loop()
        deadline = time.monotonic() + BEGIN_URL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                chunk = await loop.run_in_executor(
                    None, self._read_chunk, 0.5)
            except Exception:
                # Treat any read error as transient until the deadline
                # — the CLI may be mid-spawn.
                chunk = b""
            if chunk:
                self._buf.extend(chunk)
                m = _URL_RE.search(self._buf)
                if m:
                    self.authorize_url = m.group(0).decode("utf-8", "replace")
                    log.info("oauth_login[%s]: captured URL (len=%d)",
                             self.account_name, len(self.authorize_url))
                    return self.authorize_url
            # Heuristic: prompt-hint visible but URL not yet captured →
            # something is wrong (e.g. CLI version changed the URL
            # format); dump buffer and abort so the operator sees the
            # raw output.
            if _PROMPT_HINT in self._buf and not self.authorize_url:
                tail = bytes(self._buf[-1500:]).decode("utf-8", "replace")
                log.warning("oauth_login[%s]: prompt visible but URL "
                            "not parsed; PTY tail: %r",
                            self.account_name, tail)
        # Timed out. Log the PTY tail (server-only) and surface a
        # generic message — the tail can contain operator-pasted
        # secrets if the timeout races code submission, and the error
        # message propagates to HTTP responses.
        tail = bytes(self._buf[-1500:]).decode("utf-8", "replace")
        log.warning("oauth_login[%s]: begin() timeout; PTY tail: %r",
                    self.account_name, tail)
        self._cleanup()
        raise RuntimeError(
            f"claude auth login did not print authorize URL within "
            f"{BEGIN_URL_TIMEOUT_SECONDS:.0f}s (check container logs)")

    def _read_chunk(self, timeout: float) -> bytes:
        """Blocking read of up to 8 KiB from the PTY, with a select-style
        timeout. Returns b'' on no data."""
        assert self.proc is not None
        import select
        try:
            r, _, _ = select.select([self.proc.fd], [], [], timeout)
        except (OSError, ValueError):
            return b""
        if not r:
            return b""
        try:
            return os.read(self.proc.fd, 8192)
        except OSError:
            return b""

    async def finish(self, code: str) -> dict[str, Any]:
        """Write the operator-pasted code into the PTY, wait for the
        CLI to exit, then move .credentials.json + .claude.json into
        the destination dir. Returns a summary dict with the moved
        file paths."""
        if self.proc is None:
            raise RuntimeError("login flow not started (no PTY proc)")
        if not code or not code.strip():
            raise ValueError("code is empty")
        # Strip CRs the operator's paste may have brought along — claude
        # CLI reads a single LF-terminated line as the code; CRLF can
        # confuse the line reader.
        code = code.strip().replace("\r", "")
        try:
            self.proc.write((code + "\n").encode())
        except Exception as e:
            self._cleanup()
            raise RuntimeError(f"failed to send code to PTY: {e}")

        # Drain stdout while the CLI does the token exchange. We don't
        # parse the success message — the on-disk credentials file is
        # the source of truth. Read until process exits or timeout.
        loop = asyncio.get_running_loop()
        deadline = time.monotonic() + FINISH_TIMEOUT_SECONDS
        while self.proc.isalive() and time.monotonic() < deadline:
            chunk = await loop.run_in_executor(None, self._read_chunk, 0.5)
            if chunk:
                self._buf.extend(chunk)
        # NOTE on PTY tail handling below: the buffer holds the
        # operator's just-typed authorization code (echoed by the CLI).
        # We must NOT include the tail in any RuntimeError message,
        # because admin.py turns RuntimeError into an HTTP 500 response
        # body that flows through nginx logs and the operator's browser.
        # All tail dumps go to the server log only.
        if self.proc.isalive():
            try:
                self.proc.terminate(force=True)
            except Exception:
                pass
            tail = bytes(self._buf[-2000:]).decode("utf-8", "replace")
            log.warning("oauth_login[%s]: finish() timeout; PTY tail: %r",
                        self.account_name, tail)
            self._cleanup()
            raise RuntimeError(
                f"claude auth login did not exit within "
                f"{FINISH_TIMEOUT_SECONDS:.0f}s after code submission "
                f"(check container logs)")

        exit_status = self.proc.exitstatus
        if exit_status not in (0, None):
            tail = bytes(self._buf[-2000:]).decode("utf-8", "replace")
            log.warning("oauth_login[%s]: finish() exit=%d; PTY tail: %r",
                        self.account_name, exit_status, tail)
            self._cleanup()
            raise RuntimeError(
                f"claude auth login exited with status {exit_status} "
                f"(check container logs)")

        # Verify on-disk artifacts exist before moving — gives a clearer
        # error than copying an empty .claude/ would.
        cred_src = self.tmp_home / ".claude" / ".credentials.json"
        marker_src = self.tmp_home / ".claude.json"
        if not cred_src.is_file():
            tail = bytes(self._buf[-2000:]).decode("utf-8", "replace")
            log.warning("oauth_login[%s]: .credentials.json missing after "
                        "login (expected at %s); PTY tail: %r",
                        self.account_name, cred_src, tail)
            self._cleanup()
            raise RuntimeError(
                ".credentials.json missing after login "
                "(check container logs)")

        # Move into the destination dir. Workers symlink ~/.claude →
        # dest_dir, so the structure must be:
        #   dest_dir/.credentials.json   ← OAuth tokens
        #   dest_dir/.claude.json        ← oauthAccount marker (email
        #                                  / accountUuid / organizationUuid),
        #                                  required by worker _seed_home
        # Anything else under .claude/ (projects/ etc.) is copied too.
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        # Copy the entire .claude/ contents into dest_dir at top level.
        # We don't use shutil.move on the dir because dest_dir may
        # already exist (e.g. operator pre-created it); we want a merge.
        src_claude = self.tmp_home / ".claude"
        if src_claude.is_dir():
            for child in src_claude.iterdir():
                target = self.dest_dir / child.name
                if target.exists():
                    if target.is_dir():
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                shutil.move(str(child), str(target))
        if marker_src.is_file():
            target = self.dest_dir / ".claude.json"
            if target.exists():
                target.unlink()
            shutil.move(str(marker_src), str(target))
            # `claude auth login --claudeai` only writes the OAuth
            # bookkeeping (oauthAccount block, userID, expiresAt). It
            # does NOT set hasCompletedOnboarding, so when a fresh
            # worker boots against this dir claude CLI shows its
            # first-run "Select login method" picker — which eats
            # trigger() keystrokes and times the prewarm out. Patch
            # the file here so every worker that seeds from this dest
            # skips onboarding. Atomic write so a concurrent worker
            # read can't see a partial JSON.
            _ensure_onboarding_complete(target)

        # Permissions: container runs as uid 1000, but tempfile.mkdtemp
        # uses the current process uid. Since we're spawned by the proxy
        # (already uid 1000), files inherit that uid — no chown needed.

        result = {
            "credentials_path": str(self.dest_dir / ".credentials.json"),
            "marker_path": str(self.dest_dir / ".claude.json"),
        }
        log.info("oauth_login[%s]: credentials installed at %s",
                 self.account_name, self.dest_dir)
        self._cleanup()
        return result

    def abort(self) -> None:
        """Kill the PTY + scrub the tmpdir. Called by the sweeper on
        expiry and by the admin endpoint on explicit cancellation."""
        log.info("oauth_login[%s]: aborting flow", self.account_name)
        self._cleanup()

    def _cleanup(self) -> None:
        if self.proc is not None:
            try:
                if self.proc.isalive():
                    self.proc.terminate(force=True)
            except Exception:
                pass
            self.proc = None
        try:
            if self.tmp_home.is_dir():
                shutil.rmtree(self.tmp_home, ignore_errors=True)
        except Exception:
            pass


class LoginRegistry:
    """Per-process registry of in-progress LoginSessions, keyed by
    account name. Holds a sweeper task that drops expired flows."""

    def __init__(self) -> None:
        self._sessions: dict[str, LoginSession] = {}
        self._lock = asyncio.Lock()

    def has(self, account_name: str) -> bool:
        return account_name in self._sessions

    async def begin(self, account_name: str, dest_dir: Path,
                     claude_binary: str = "claude",
                     pool: str | None = None,
                     priority: int | None = None) -> str:
        async with self._lock:
            if account_name in self._sessions:
                raise RuntimeError(
                    f"a login flow for account {account_name!r} is "
                    f"already in progress; abort it first or pick a "
                    f"different name")
            sess = LoginSession(account_name, dest_dir, claude_binary,
                                pool=pool, priority=priority)
            try:
                url = await sess.begin()
            except Exception:
                # begin() already cleaned up on failure
                raise
            self._sessions[account_name] = sess
            return url

    def pool_for(self, account_name: str) -> str | None:
        """Return the operator-selected sk-key for an in-progress login.
        Used by the finish endpoint so the pool choice survives the
        round-trip without the dashboard needing to echo it back."""
        sess = self._sessions.get(account_name)
        return sess.pool if sess is not None else None

    def priority_for(self, account_name: str) -> int | None:
        """Return the operator-selected routing tier for an in-progress
        login. Same round-trip pattern as pool_for. None means "use the
        AccountConfig default" (100)."""
        sess = self._sessions.get(account_name)
        return sess.priority if sess is not None else None

    async def finish(self, account_name: str, code: str) -> dict[str, Any]:
        async with self._lock:
            sess = self._sessions.pop(account_name, None)
        if sess is None:
            raise KeyError(account_name)
        return await sess.finish(code)

    async def abort(self, account_name: str) -> bool:
        async with self._lock:
            sess = self._sessions.pop(account_name, None)
        if sess is None:
            return False
        sess.abort()
        return True

    async def sweep(self) -> int:
        """Drop expired sessions. Returns the number swept."""
        dropped = 0
        async with self._lock:
            for name in list(self._sessions.keys()):
                if self._sessions[name].is_expired():
                    self._sessions.pop(name).abort()
                    dropped += 1
        return dropped

    async def run_sweeper(self) -> None:
        """Background task: every 30s, drop expired flows."""
        while True:
            try:
                await asyncio.sleep(30.0)
                n = await self.sweep()
                if n:
                    log.info("oauth_login sweeper dropped %d expired flow(s)", n)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("oauth_login sweeper tick failed")
