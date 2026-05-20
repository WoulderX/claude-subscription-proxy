"""Per-user session = a `src.worker` subprocess holding its own mitm +
claude PTY. The server talks to it over stdin/stdout JSON lines."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from ..config import Config
from .state import ResponseChannel

log = logging.getLogger(__name__)


class ClaudeSession:
    """Owns the worker subprocess for one user. Serialises requests with
    a per-session asyncio lock — one outbound /v1/messages per claude
    process at a time."""

    def __init__(self, user_id: str, mitm_port: int, config: Config) -> None:
        self.user_id = user_id
        self.mitm_port = mitm_port
        self.config = config
        self.lock = asyncio.Lock()  # one in-flight request per user
        self.last_used = time.monotonic()
        self.started_at = time.monotonic()

        self.proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._next_req_id = 0
        self._channels: dict[int, ResponseChannel] = {}
        self._closed = False

    async def start(self) -> None:
        home = self.config.user_home(self.user_id).resolve()
        home.mkdir(parents=True, exist_ok=True)
        self._seed_home(home)

        worker_env = os.environ.copy()
        # Worker emits structured logs on stderr; keep level configurable
        # via the same env var the server uses.
        worker_env["PYTHONUNBUFFERED"] = "1"

        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", "-m", "src.worker",
            "--user-id", self.user_id,
            "--mitm-port", str(self.mitm_port),
            "--home", str(home),
            "--ca-cert", str(self.config.ca_cert_path()),
            "--claude-binary", self.config.claude.binary,
            "--log-level", os.environ.get("LOG_LEVEL", "INFO"),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=None,  # inherit — worker logs flow to server stderr
            env=worker_env,
        )
        assert self.proc.stdout is not None

        # First line must be {"type": "ready"} — wait for it (with timeout)
        # so a hung worker doesn't block the API request indefinitely.
        try:
            ready_line = await asyncio.wait_for(
                self.proc.stdout.readline(), timeout=60)
        except asyncio.TimeoutError:
            self.proc.kill()
            raise RuntimeError(
                f"worker for {self.user_id} did not signal ready within 60s")
        if not ready_line:
            raise RuntimeError(
                f"worker for {self.user_id} exited before signalling ready")
        msg = json.loads(ready_line)
        if msg.get("type") != "ready":
            raise RuntimeError(
                f"worker for {self.user_id} sent unexpected first line: {msg}")

        self._reader_task = asyncio.create_task(
            self._read_loop(), name=f"session-reader-{self.user_id}")
        log.info("session up user=%s worker_pid=%s mitm_port=%s",
                 self.user_id, self.proc.pid, self.mitm_port)

    def _seed_home(self, home: Path) -> None:
        """Populate the user's isolated HOME from the operator's HOME so
        claude code skips its first-run onboarding (theme picker, etc.)
        and uses the operator's OAuth credentials.

          - $HOME/.claude.json            -> copied once (onboarding marker;
            claude CLI mutates it heavily, so each user keeps a private one)
          - $HOME/.claude/.credentials.json -> SYMLINKED to the operator's
            file. Every worker shares one credentials source; no per-user
            copy is ever kept. A refreshed / re-logged-in operator token is
            picked up automatically on the next worker start, with no stale
            copy left behind to purge.

        Transcripts, sessions, telemetry stay isolated per-user."""
        op_home = Path(os.path.expanduser("~"))
        marker = home / ".claude.json"
        creds_dst = home / ".claude" / ".credentials.json"
        creds_dst.parent.mkdir(parents=True, exist_ok=True)

        src_marker = op_home / ".claude.json"
        src_creds = op_home / ".claude" / ".credentials.json"
        if src_marker.is_file() and not marker.exists():
            shutil.copy2(src_marker, marker)
            log.info("seeded %s/.claude.json", home)

        # Credentials: (re)point a symlink at the operator's file on every
        # start. Unlink whatever is there first — a stale copy from an older
        # build, a dangling link, or a regular file a claude-CLI token
        # refresh may have written over the link — so the worker always
        # reads the single live operator credential.
        if src_creds.is_file():
            if creds_dst.is_symlink() or creds_dst.exists():
                creds_dst.unlink()
            creds_dst.symlink_to(src_creds)
            log.info("linked %s/.claude/.credentials.json -> %s",
                     home, src_creds)
        else:
            log.warning("operator credentials missing at %s", src_creds)

    async def stop(self) -> None:
        self._closed = True
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
            except ProcessLookupError:
                pass
        if self._reader_task:
            self._reader_task.cancel()
        # Drain any waiting channels so callers don't hang.
        for ch in self._channels.values():
            ch.queue.put_nowait(None)
        self._channels.clear()
        log.info("session stopped user=%s", self.user_id)

    async def call(self, body: dict[str, Any]) -> ResponseChannel:
        """Submit a /v1/messages body. Returns a channel streaming the
        Anthropic SSE response bytes verbatim. Lock holds until the
        request enters the worker — releasing the lock while the worker
        is still streaming back would let a second caller race with the
        first, since the worker can only serve one model call at a time.

        The proc-alive check is inside the lock so a request arriving
        during a scheduled restart waits for the new worker rather than
        racing with the dead one."""
        async with self.lock:
            if self.proc is None or self.proc.returncode is not None:
                raise RuntimeError(f"worker for {self.user_id} not running")
            self.last_used = time.monotonic()
            req_id = self._next_req_id
            self._next_req_id += 1
            channel = ResponseChannel()
            self._channels[req_id] = channel

            assert self.proc.stdin is not None
            line = json.dumps({"type": "request", "id": req_id,
                               "body": body}) + "\n"
            self.proc.stdin.write(line.encode())
            await self.proc.stdin.drain()
            log.info("user=%s submitted req id=%d", self.user_id, req_id)
            return channel

    async def _read_loop(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        try:
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    log.info("worker stdout closed user=%s", self.user_id)
                    return
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("malformed worker stdout: %r", line[:200])
                    continue
                req_id = msg.get("id")
                t = msg.get("type")
                channel = self._channels.get(req_id)
                if channel is None:
                    continue
                if t == "chunk":
                    try:
                        data = base64.b64decode(msg.get("data", ""))
                    except Exception:
                        data = b""
                    if data:
                        channel.queue.put_nowait(data)
                elif t == "end":
                    channel.queue.put_nowait(None)
                    self._channels.pop(req_id, None)
                elif t == "error":
                    log.error("user=%s worker error: %s",
                              self.user_id, msg.get("msg"))
                    channel.queue.put_nowait(None)
                    self._channels.pop(req_id, None)
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("reader loop crashed user=%s", self.user_id)
        finally:
            # If worker died, flush every waiting channel.
            for ch in list(self._channels.values()):
                ch.queue.put_nowait(None)
            self._channels.clear()

    def idle_seconds(self) -> float:
        return time.monotonic() - self.last_used

    def age_seconds(self) -> float:
        return time.monotonic() - self.started_at

    async def restart(self) -> None:
        """Tear down the current worker subprocess and spin a fresh one
        on the same mitm port. Caller MUST hold self.lock so no new
        request is submitted mid-restart. In-flight SSE streams whose
        bytes were already in transit get truncated (stop() drains their
        channels with a None sentinel)."""
        await self.stop()
        self._closed = False
        self.proc = None
        self._reader_task = None
        self._next_req_id = 0
        self._channels = {}
        self.started_at = time.monotonic()
        self.last_used = time.monotonic()
        await self.start()
