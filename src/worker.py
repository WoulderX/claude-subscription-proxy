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
                 home: Path, claude_binary: str, ca_cert: Path) -> None:
        self.user_id = user_id
        self.mitm_port = mitm_port
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
                                       timeout=30)
            except asyncio.TimeoutError:
                log.warning("user=%s mitm did not intercept within 30s",
                            self.user_id)
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
    reader = asyncio.StreamReader()
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
