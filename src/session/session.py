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


def _summarize_body(
    body: dict[str, Any],
    request_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Produce a small log-safe summary of a request body for /status,
    so operators can see what a worker is processing without expanding
    the full JSON. Truncates user content to 80 chars to bound response
    size and avoid dumping full prompts into a health endpoint.

    request_metadata is merged into the top-level summary (typically
    holds {"litellm": {...}} extracted from x-litellm-* request headers
    so the operator can attribute the in-flight task to the original
    LiteLLM virtual user — by default the proxy only sees LiteLLM's
    upstream API key, not the end user behind it)."""
    if not isinstance(body, dict):
        return dict(request_metadata) if request_metadata else {}
    msgs = body.get("messages") if isinstance(body.get("messages"), list) else []
    last_user_text = ""
    for m in reversed(msgs):
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        content = m.get("content")
        if isinstance(content, str):
            last_user_text = content
        elif isinstance(content, list):
            for blk in content:
                if isinstance(blk, dict) and blk.get("type") == "text":
                    last_user_text = blk.get("text", "")
                    break
        break
    preview = last_user_text[:80]
    if len(last_user_text) > 80:
        preview += "…"
    summary: dict[str, Any] = {
        "model": body.get("model"),
        "max_tokens": body.get("max_tokens"),
        "stream": bool(body.get("stream")),
        "n_messages": len(msgs),
        "last_user_preview": preview,
    }
    if request_metadata:
        summary.update(request_metadata)
    return summary


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
            "--mitm-intercept-timeout",
                str(self.config.claude.timeouts.mitm_intercept_seconds),
            "--response-stall-timeout",
                str(self.config.claude.timeouts.response_stall_seconds),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=None,  # inherit — worker logs flow to server stderr
            env=worker_env,
            # Worker streams SSE chunks back as base64-wrapped JSON lines.
            # A single Anthropic chunk over ~48 KB raw bytes (≈ 64 KB after
            # base64 + JSON envelope) overflows the default StreamReader
            # buffer, _read_loop raises LimitOverrunError, the read coroutine
            # dies and the worker process exits. Large tool_use inputs and
            # long text_delta blocks hit this regularly. 16 MiB has room
            # for anything Anthropic emits in one chunk.
            limit=16 * 1024 * 1024,
        )
        assert self.proc.stdout is not None

        # First line must be {"type": "ready"} — wait for it (with timeout)
        # so a hung worker doesn't block the API request indefinitely.
        try:
            ready_line = await asyncio.wait_for(
                self.proc.stdout.readline(),
                timeout=self.config.claude.timeouts.worker_ready_seconds)
        except asyncio.TimeoutError:
            self.proc.kill()
            raise RuntimeError(
                f"worker for {self.user_id} did not signal ready within "
                f"{self.config.claude.timeouts.worker_ready_seconds:.0f}s")
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
        """Populate the user's isolated HOME so claude code skips its
        first-run onboarding and shares the operator's OAuth credentials.

          - $HOME/.claude.json  -> copied once. claude CLI mutates this
            heavily per session, so each user keeps a private copy to
            avoid concurrent-write races between workers.
          - $HOME/.claude/      -> SYMLINKED (entire directory) to the
            operator's ~/.claude/. Sharing at the directory level means a
            claude-CLI atomic token refresh (write tmp + rename) happens
            *inside* the shared directory — the new file lands directly
            at the operator's source path. All other workers transparently
            read the rotated token on their next call; no copy-back or
            propagation logic is needed. Per-user transcript isolation
            still works because claude stores sessions under cwd-encoded
            subdirs (.claude/projects/<encoded-cwd>/sessions/), and each
            worker's cwd is its own HOME."""
        op_home = Path(os.path.expanduser("~")).resolve()
        marker = home / ".claude.json"
        src_marker = op_home / ".claude.json"
        if src_marker.is_file() and not marker.exists():
            shutil.copy2(src_marker, marker)
            log.info("seeded %s/.claude.json", home)

        claude_dir = home / ".claude"
        src_claude = op_home / ".claude"
        if not src_claude.is_dir():
            log.warning("operator .claude/ missing at %s", src_claude)
            return

        # Idempotent: leave a correct symlink alone, otherwise rebuild it.
        # The legacy branch handles upgrades from the file-symlink build
        # where a token-refresh rename had replaced the link with a real
        # dir/file holding a now-stale credential.
        if claude_dir.is_symlink():
            try:
                current = Path(os.readlink(claude_dir))
            except OSError:
                current = None
            if current == src_claude:
                return
            claude_dir.unlink()
        elif claude_dir.is_dir():
            log.info("migrating legacy per-user %s into directory symlink",
                     claude_dir)
            shutil.rmtree(claude_dir)
        elif claude_dir.exists():
            claude_dir.unlink()

        claude_dir.symlink_to(src_claude)
        log.info("linked %s -> %s (shared with operator)",
                 claude_dir, src_claude)

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

    async def call(
        self,
        body: dict[str, Any],
        request_metadata: dict[str, Any] | None = None,
    ) -> ResponseChannel:
        """Submit a /v1/messages body. Returns a channel streaming the
        Anthropic SSE response bytes verbatim. Lock holds until the
        request enters the worker — releasing the lock while the worker
        is still streaming back would let a second caller race with the
        first, since the worker can only serve one model call at a time.

        The proc-alive check is inside the lock so a request arriving
        during a scheduled restart waits for the new worker rather than
        racing with the dead one.

        request_metadata is opaque side-info (e.g. forwarded
        x-litellm-* headers) merged into the channel's body_summary so
        operators can attribute the task on /status without our IPC
        having to know what's in it."""
        async with self.lock:
            return await self._submit(body, request_metadata)

    async def _submit(
        self,
        body: dict[str, Any],
        request_metadata: dict[str, Any] | None = None,
    ) -> ResponseChannel:
        """Lock-free body submission. Caller MUST hold self.lock (or
        guarantee single-writer access some other way). Exists so that
        prewarm flows already running under a restart-held lock can
        submit the dummy bootstrap request without trying to re-enter
        the lock, and without releasing it (which would let a real
        user request race in before bootstrap has populated the
        per-process feature-flag cache)."""
        if self.proc is None or self.proc.returncode is not None:
            raise RuntimeError(f"worker for {self.user_id} not running")
        self.last_used = time.monotonic()
        req_id = self._next_req_id
        self._next_req_id += 1
        channel = ResponseChannel(
            body_summary=_summarize_body(body, request_metadata))
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
                        channel.last_chunk_at = time.monotonic()
                        channel.bytes_received += len(data)
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
