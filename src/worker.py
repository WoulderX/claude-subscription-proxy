"""Per-user session worker.

Runs as a subprocess of the FastAPI server. Owns:
  - a mitmproxy DumpMaster on a per-user loopback port
  - a `claude` TUI driven via PTY
  - its own asyncio event loop (so the embedded DumpMaster + PTY drain
    don't have to coexist with the API server's loop, which seems to
    deadlock the hijack flow on Linux + uvloop/asyncio mixing)

IPC protocol — JSON lines on stdin (in) / stdout (out):

  ---> {"type":"request","id":<int>,"body":{<anthropic /v1/messages body>}}
  <--- {"type":"chunk","id":<int>,"data":"<base64 raw SSE bytes>"}
  <--- {"type":"end","id":<int>}
  <--- {"type":"error","id":<int>,"msg":"..."}

  At startup, before reading any requests:
  <--- {"type":"ready"}

Requests are processed one at a time per worker (per user). The server
side serialises by holding a per-session asyncio lock.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

# Worker reuses the same MitmRunner + ClaudePtyDriver + addon code as the
# old in-process design. They just run inside this subprocess now.
from .mitm.runner import MitmRunner
from .pty_driver import ClaudePtyDriver
from .session.state import PendingRequest, ResponseChannel


log = logging.getLogger("worker")


class WorkerSession:
    """Same shape as the old ClaudeSession (HijackAddon back-references
    `session.pending` and `session.response`), but lives entirely inside
    the worker subprocess."""

    def __init__(self, user_id: str, mitm_port: int,
                 home: Path, claude_binary: str, ca_cert: Path,
                 mitm_intercept_timeout: float = 30.0,
                 response_stall_timeout: float = 90.0) -> None:
        self.user_id = user_id
        self.mitm_port = mitm_port
        self.mitm_intercept_timeout = mitm_intercept_timeout
        # Read by HijackAddon to arm a per-flow watchdog; if a chunk
        # doesn't arrive within this window the channel is force-closed
        # to recover from upstream-hung-after-tiny-error patterns.
        self.response_stall_timeout = response_stall_timeout
        self.lock = asyncio.Lock()

        self.pending: PendingRequest | None = None
        self.response: ResponseChannel | None = None

        self.mitm = MitmRunner(port=mitm_port, session=self)
        self.pty = ClaudePtyDriver(
            binary=claude_binary,
            home=home,
            https_proxy=f"http://127.0.0.1:{mitm_port}",
            ca_cert=ca_cert,
        )

    async def start(self) -> None:
        await self.mitm.start()
        await self.pty.start()
        log.info("worker session started user=%s mitm_port=%s",
                 self.user_id, self.mitm_port)

    async def stop(self) -> None:
        try:
            await self.pty.stop()
        finally:
            await self.mitm.stop()

    async def call(self, body: dict[str, Any]) -> ResponseChannel:
        async with self.lock:
            self.response = ResponseChannel()
            self.pending = PendingRequest(body=body)
            await self.pty.trigger()
            try:
                await asyncio.wait_for(self.pending.consumed.wait(),
                                       timeout=self.mitm_intercept_timeout)
            except asyncio.TimeoutError:
                # mitm never saw a /v1/messages it could claim — most
                # often claude TUI is in a "rate limited / error" UI
                # state that swallowed our keystroke instead of firing
                # a model call. The channel will *never* receive bytes
                # (mitm's _tap is what feeds it, and _tap wasn't set
                # because the flow wasn't hijacked), so without an
                # explicit close the caller's `async for chunk in
                # channel.iter()` hangs forever, leaks the channel into
                # the server's _channels dict, and the worker looks
                # busy forever even though nothing is happening.
                log.warning("user=%s mitm did not intercept within %.0fs; "
                            "closing response and clearing pending slot",
                            self.user_id, self.mitm_intercept_timeout)
                self.pending = None
                await self.response.put(None)
            return self.response


async def _send(line_obj: dict[str, Any]) -> None:
    """Write one JSON line to stdout (line-buffered for streaming)."""
    payload = json.dumps(line_obj, ensure_ascii=False) + "\n"
    sys.stdout.write(payload)
    sys.stdout.flush()


async def _handle(session: WorkerSession, req_id: int, body: dict[str, Any]) -> None:
    """Execute one request to completion: trigger PTY, stream chunks back."""
    try:
        channel = await session.call(body)
    except Exception as e:
        log.exception("user=%s call failed", session.user_id)
        await _send({"type": "error", "id": req_id, "msg": str(e)})
        return
    try:
        async for chunk in channel.iter():
            if not chunk:
                continue
            await _send({"type": "chunk", "id": req_id,
                         "data": base64.b64encode(chunk).decode("ascii")})
    except Exception as e:
        log.exception("user=%s stream relay failed", session.user_id)
        await _send({"type": "error", "id": req_id, "msg": str(e)})
        return
    await _send({"type": "end", "id": req_id})


async def _stdin_lines() -> "asyncio.StreamReader":
    loop = asyncio.get_running_loop()
    # 16 MiB buffer: a request body with a large system prompt or many
    # messages can exceed the asyncio StreamReader default of 64 KiB,
    # causing readline() to raise LimitOverrunError and the worker to
    # die. Match the server-side stdout limit (src/session/session.py).
    reader = asyncio.StreamReader(limit=16 * 1024 * 1024)
    proto = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: proto, sys.stdin)
    return reader


async def amain() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--mitm-port", type=int, required=True)
    parser.add_argument("--home", required=True)
    parser.add_argument("--claude-binary", default="claude")
    parser.add_argument("--ca-cert", required=True)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--mitm-intercept-timeout", type=float, default=30.0,
                        help="seconds to wait for mitm to hijack TUI's "
                             "outbound /v1/messages before failing the call")
    parser.add_argument("--response-stall-timeout", type=float, default=90.0,
                        help="seconds with no new SSE chunk after which the "
                             "mitm watchdog force-closes the response "
                             "channel (recovers from upstream hangs)")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s [worker:%(name)s:" + args.user_id + "] %(message)s",
        stream=sys.stderr,
    )

    session = WorkerSession(
        user_id=args.user_id,
        mitm_port=args.mitm_port,
        home=Path(args.home),
        claude_binary=args.claude_binary,
        ca_cert=Path(args.ca_cert),
        mitm_intercept_timeout=args.mitm_intercept_timeout,
        response_stall_timeout=args.response_stall_timeout,
    )
    await session.start()
    await _send({"type": "ready"})

    reader = await _stdin_lines()
    tasks: set[asyncio.Task] = set()
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                log.warning("malformed stdin line: %r", line[:200])
                continue
            if msg.get("type") != "request":
                continue
            req_id = msg.get("id")
            body = msg.get("body") or {}
            t = asyncio.create_task(_handle(session, req_id, body))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
    finally:
        for t in tasks:
            t.cancel()
        await session.stop()


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
