from __future__ import annotations

import asyncio
import logging
import os
import select
from pathlib import Path

import ptyprocess

log = logging.getLogger(__name__)

# Bytes that appear in claude code's TUI once it is ready to accept input.
# "❯ " is the prompt cursor; the placeholder hint "Try" comes alongside it.
# We wait for either to appear before sending input.
_READY_MARKERS = (b"\xe2\x9d\xaf", b"Try ")
_READY_TIMEOUT = 15.0


class ClaudePtyDriver:
    """Spawns `claude` in a PTY with HTTPS_PROXY pointing at our mitm port.
    Provides `trigger()` to submit a placeholder prompt that causes claude
    code to emit one outbound /v1/messages — its content will be replaced
    by the mitm addon before it leaves the loopback.

    We deliberately drop all TUI output (no scraping) — the model's real
    response is read directly off the network via mitm."""

    def __init__(self, binary: str, home: Path, https_proxy: str, ca_cert: Path) -> None:
        self.binary = binary
        self.home = home
        self.https_proxy = https_proxy
        self.ca_cert = ca_cert
        self.proc: ptyprocess.PtyProcess | None = None
        self._drain_task: asyncio.Task | None = None

    async def start(self) -> None:
        env = os.environ.copy()
        env["HOME"] = str(self.home)
        env["HTTPS_PROXY"] = self.https_proxy
        env["HTTP_PROXY"] = self.https_proxy
        env["NODE_EXTRA_CA_CERTS"] = str(self.ca_cert)
        # Strip env vars that mark THIS process as a claude-code child; we
        # want the new subprocess to look like an independent CLI launch.
        for k in ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID",
                  "CLAUDE_CODE_ENTRYPOINT", "AI_AGENT"):
            env.pop(k, None)

        # Spawn synchronously — ptyprocess.PtyProcess.spawn is fast and
        # going through run_in_executor leaves the PTY fd associated with
        # the worker thread's handler in a way that subsequent writes
        # from the main asyncio thread are silently swallowed.
        self.proc = ptyprocess.PtyProcess.spawn(
            [self.binary],
            env=env,
            cwd=str(self.home),
            dimensions=(40, 120),
        )
        loop = asyncio.get_running_loop()
        # Wait until the TUI reaches the input prompt before we start the
        # background drain task. Claude code 2.1.144 takes 5-8s on a cold
        # boot to fetch bootstrap + MCP registry + paint the prompt.
        await loop.run_in_executor(None, self._wait_until_ready)
        self._drain_task = asyncio.create_task(self._drain())

    def _wait_until_ready(self) -> None:
        import time
        assert self.proc is not None
        fd = self.proc.fd
        seen = b""
        ready_at: float | None = None
        deadline = time.monotonic() + _READY_TIMEOUT
        # Two-phase: (1) wait until prompt marker appears, then (2) keep
        # draining for an additional 2.5s so Ink finishes its mount cycle.
        # Without (2), keystrokes sent immediately after the prompt paints
        # tend to be lost.
        while time.monotonic() < deadline:
            r, _, _ = select.select([fd], [], [], 0.2)
            if r:
                try:
                    chunk = os.read(fd, 8192)
                except OSError:
                    break
                if chunk:
                    seen += chunk
            if ready_at is None and any(m in seen for m in _READY_MARKERS):
                ready_at = time.monotonic()
                log.info("pty TUI prompt visible after %d bytes; draining "
                         "for 2.5s to let Ink bind input handlers",
                         len(seen))
            if ready_at is not None and time.monotonic() - ready_at >= 2.5:
                log.info("pty TUI ready, total boot bytes=%d", len(seen))
                return
        log.warning("pty TUI not ready within %.1fs; sending input anyway",
                    _READY_TIMEOUT)

    async def stop(self) -> None:
        if self._drain_task:
            self._drain_task.cancel()
        if self.proc and self.proc.isalive():
            try:
                self.proc.terminate(force=True)
            except Exception:
                pass

    async def trigger(self, placeholder: str = "say hi") -> None:
        """Type a placeholder + submit so claude emits one outbound
        request. The placeholder is thrown away — mitm swaps the real body.
        Multi-char strings are more reliable than single chars (claude code
        2.1.144 sometimes ignores 1-char submissions)."""
        if not self.proc or not self.proc.isalive():
            raise RuntimeError("claude PTY not alive")
        # Use ptyprocess.write which goes through its buffered file
        # object + flush — equivalent to what the probe_trigger.py probe
        # did successfully.
        data = (placeholder + "\r").encode()
        self.proc.write(data)
        log.info("pty trigger wrote %d bytes via ptyprocess: %r",
                 len(data), data)

    async def _drain(self) -> None:
        """Continuously read+discard TUI output so the PTY buffer doesn't
        fill up and block the child. Uses select+os.read (non-blocking
        polling) so we never tie up an executor thread waiting on a
        blocking ptyprocess.read()."""
        assert self.proc is not None
        fd = self.proc.fd
        loop = asyncio.get_running_loop()
        while True:
            try:
                # Yield to the loop, then drain whatever is ready.
                await asyncio.sleep(0.1)
                r, _, _ = select.select([fd], [], [], 0)
                while r:
                    try:
                        data = os.read(fd, 8192)
                    except BlockingIOError:
                        break
                    except OSError:
                        return
                    if not data:
                        return
                    r, _, _ = select.select([fd], [], [], 0)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("pty drain error")
                return
