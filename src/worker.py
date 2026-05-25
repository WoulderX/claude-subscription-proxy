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
from .rate_limit import classify_rate_limit_reason, parse_reset_time
from .session.state import PendingRequest, ResponseChannel


log = logging.getLogger("worker")


class WorkerSession:
    """Same shape as the old ClaudeSession (HijackAddon back-references
    `session.pending` and `session.response`), but lives entirely inside
    the worker subprocess."""

    def __init__(self, user_id: str, mitm_port: int,
                 home: Path, claude_binary: str, ca_cert: Path,
                 mitm_intercept_timeout: float = 90.0,
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
        # Side-channel for addon → main-process events that aren't tied
        # to a specific in-flight request id (e.g. HTTP 429 details
        # parsed from response headers). worker.amain drains this and
        # ships entries up via stdout JSON.
        self.events: asyncio.Queue[dict] = asyncio.Queue()

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
                #
                # Dump the last ~4KB of TUI screen content so an operator
                # can SEE what modal / prompt / error the TUI was showing
                # when it ignored our keystroke. Without this we're
                # blind: claude CLI process is alive, drain loop is
                # consuming bytes, but we don't know what the user-
                # facing screen says.
                screen_tail = self.pty.dump_screen_tail()
                matcher_view = self.pty.dump_recent_chunks_squeezed()
                # Which of the known _DISMISS_RULES are currently
                # matching? If NONE, the modal blocking input is one
                # we don't have a rule for yet — eyeball the matcher
                # view above and add it to ClaudePtyDriver._DISMISS_RULES.
                markers_present = self.pty.matching_modal_names()
                log.warning(
                    "user=%s mitm did not intercept within %.0fs; "
                    "closing response and clearing pending slot.\n"
                    "──── PTY screen tail (last 16KB, ANSI stripped) ────\n"
                    "%s\n"
                    "──── end PTY screen tail ────\n"
                    "──── matcher view (recent_chunks 4KB, squeezed) ────\n"
                    "%s\n"
                    "──── end matcher view ────\n"
                    "matcher anchors present in view: %s",
                    self.user_id, self.mitm_intercept_timeout,
                    screen_tail or "(empty — TUI produced no recent output)",
                    matcher_view or "(empty)",
                    markers_present or "NONE")
                # Try to recover Anthropic's official reset timestamp
                # from the TUI modal ("You've hit your limit · resets
                # May 27, 12am (UTC)"). When found, emit a structured
                # IPC so the main process records the exact account-
                # unblock moment instead of guessing a window.
                #
                # Three places to look (in order):
                #   1. The PTY's "rate_limit_snippet" — a frozen ±512B
                #      window around the first "resets MMM DD" we ever
                #      saw on this PTY. Survives later redraws, so it
                #      catches modals that flash briefly at startup.
                #   2. The 16KB ANSI-stripped screen tail (preserves
                #      spaces; matches the typical regex).
                #   3. The 4KB whitespace-squeezed matcher view (last
                #      resort if the modal lost its spaces to TUI
                #      redraw artifacts).
                rl_snippet = self.pty.dump_rate_limit_snippet()
                # Don't mistake the /usage slash-command screen for a
                # rate-limit modal: /usage renders "Resets MMM DD,
                # Xpm (UTC)" for the 5h + weekly quota windows, which
                # parse_reset_time happily matches and was previously
                # tagged as weekly_limit, marking the whole account
                # offline for days. The /usage tab bar marker is
                # specific enough that no real rate-limit modal carries
                # it. When detected, skip the reset parse entirely —
                # the underlying timeout is "TUI swallowed our trigger"
                # (a probe-cleanup race), not "Anthropic said no".
                screen_blob = (matcher_view or "") + " " + (screen_tail or "")
                is_usage_screen = (
                    "SettingsStatusConfigUsageStats" in screen_blob
                    or ("Currentsession" in screen_blob.replace(" ", "")
                        and "Currentweek" in screen_blob.replace(" ", "")))
                if is_usage_screen:
                    log.warning(
                        "user=%s mitm-intercept timeout while TUI was on "
                        "/usage screen — skipping rate-limit parse (the "
                        "'Resets MMM DD' text on /usage is a quota display, "
                        "not a rate-limit modal)", self.user_id)
                    reset_epoch = None
                else:
                    reset_epoch = (parse_reset_time(rl_snippet)
                                   or parse_reset_time(screen_tail)
                                   or parse_reset_time(matcher_view))
                if rl_snippet:
                    log.info(
                        "rate-limit snippet captured (%d chars): %r",
                        len(rl_snippet), rl_snippet[:300])
                if reset_epoch is not None:
                    import time as _time
                    reason = classify_rate_limit_reason(
                        float(reset_epoch) - _time.time())
                    log.info("user=%s parsed official reset time epoch=%d "
                             "reason=%s", self.user_id,
                             int(reset_epoch), reason)
                    try:
                        await _send({
                            "type": "rate_limit",
                            "until_epoch": float(reset_epoch),
                            "reason": reason,
                            "source": "tui_modal",
                        })
                    except Exception:
                        log.exception("failed to emit rate_limit event")
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
    # Usage event (token counts parsed off SSE by the mitm addon) goes
    # out BEFORE the end marker so the main process can correlate it
    # with this req_id's still-registered channel — once `end` lands
    # on the main side the channel is popped from session._channels.
    if channel.usage is not None:
        try:
            payload = {"type": "usage", "id": req_id}
            payload.update(channel.usage)
            await _send(payload)
        except Exception:
            log.exception("user=%s failed sending usage event", session.user_id)
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
    parser.add_argument("--mitm-intercept-timeout", type=float, default=90.0,
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
    # mitmproxy.proxy.server logs every TCP client/server connect +
    # disconnect at INFO — with 15 workers × dozens of upstream
    # connections per minute (api.anthropic.com, npm registry, datadog,
    # etc.) this is ~95% of all container log volume in steady state
    # and crowds out anything actually interesting. Push it to WARNING.
    # Keep `mitmproxy.proxy.mode_servers` (listener bind announcements)
    # and `mitmproxy.master` (startup/shutdown) at the inherited level.
    logging.getLogger("mitmproxy.proxy.server").setLevel(logging.WARNING)
    # PTY driver chats at INFO during boot ("TUI prompt visible after N
    # bytes", "TUI ready, total boot bytes=N") × 15 workers — pure
    # noise in normal operation. Keep WARNING so the modal auto-dismiss
    # line (which indicates the TUI was about to swallow our keystroke)
    # still surfaces, along with any exception traces. Drop to INFO
    # temporarily when debugging a stuck PTY.
    logging.getLogger("src.pty_driver").setLevel(logging.WARNING)

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

    # Drain session.events into IPC — addons push to this queue when
    # they spot something useful out of band (e.g. HTTP 429 headers
    # parsed at response-time, before any body bytes arrive).
    async def _events_relay():
        while True:
            ev = await session.events.get()
            try:
                await _send(ev)
            except Exception:
                log.exception("failed to send event: %s", ev)

    events_task = asyncio.create_task(_events_relay(), name="events-relay")

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
            mtype = msg.get("type")
            if mtype == "probe_quota":
                # Fire-and-forget: type `/usage` into the TUI, then Esc
                # to dismiss the resulting screen so the next trigger
                # lands on the prompt, not on the /usage tab bar.
                # The mitm addon captures the /api/oauth/usage response
                # body while this is happening and pushes a quota_usage
                # event onto session.events; the explicit Esc here is a
                # belt-and-suspenders backup for the drain loop's
                # auto-dismiss rule (debounce shared across rules can
                # occasionally swallow the Esc keystroke).
                try:
                    await session.pty.send_slash_command("/usage")
                    log.info("probe_quota: sent /usage to TUI")
                    # 2s: enough for the HTTP call to land + screen to
                    # finish drawing. The mitm capture is independent
                    # of when we Esc — addon hooks on response complete
                    # which fires regardless of TUI screen state.
                    await asyncio.sleep(2.0)
                    if session.pty.proc and session.pty.proc.isalive():
                        session.pty.proc.write(b"\x1b")
                        log.info("probe_quota: sent Esc to dismiss /usage")
                except Exception:
                    log.exception("probe_quota: failed to send /usage")
                continue
            if mtype != "request":
                continue
            req_id = msg.get("id")
            body = msg.get("body") or {}
            t = asyncio.create_task(_handle(session, req_id, body))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
    finally:
        events_task.cancel()
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
