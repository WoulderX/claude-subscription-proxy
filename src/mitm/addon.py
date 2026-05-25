from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import TYPE_CHECKING

from mitmproxy import http

if TYPE_CHECKING:
    from ..session.session import ClaudeSession

log = logging.getLogger(__name__)

ANTHROPIC_HOSTS = {"api.anthropic.com"}
# Real claude code hits /v1/messages?beta=true — flow.request.path
# carries the query string, so we match on the bare path prefix.
MESSAGES_PATH_PREFIX = "/v1/messages"
# Path that backs the TUI `/usage` slash command. Triggered by sending
# "/usage\r" to a worker's PTY; the response is a small JSON document
# carrying 5h / 7d / 7d-per-model utilization. Captured verbatim and
# shipped to the main process via session.events — no streaming, no
# transformation.
QUOTA_USAGE_PATH = "/api/oauth/usage"

# Body fields the user payload owns: claude's originals are dropped, user's
# values win. Everything else from claude's body is preserved verbatim
# (metadata, anthropic_version, etc.) so the request keeps its
# "subscription / interactive Claude Code" fingerprint.
#
# `tools` is user-owned so callers can do real function calling against
# their own schemas (e.g. get_weather). When the caller does not send
# `tools`, claude CLI's built-in tools (Bash/Read/Edit/...) remain in
# place — the merge only replaces fields the caller explicitly set.
USER_OWNED_BODY_FIELDS = {
    "messages",
    "model",
    "max_tokens",
    "temperature",
    "top_p",
    "top_k",
    "stop_sequences",
    "stream",
    "tool_choice",
    "tools",
}

# Request headers we never touch — these encode claude code's identity
# (User-Agent, anthropic-beta, x-app, x-stainless-*, etc.) and getting
# them wrong is how usage gets bucketed into the SDK quota.
# We DO let the user override these via the body if they really need to,
# but by default we preserve everything.
PRESERVED_HEADER_PREFIXES = (
    "anthropic-",
    "x-stainless-",
    "x-app",
    "user-agent",
)

# `anthropic-beta` tokens with this prefix are removed before the request
# leaves: the 1M-context beta makes Anthropic bill the call as a premium
# "long context" request needing pay-as-you-go credits, which a Claude
# subscription does not cover. Feature flag, not an identity fingerprint.
STRIP_BETA_PREFIX = "context-1m"


class _UsageScanner:
    """Streaming SSE parser that pulls token usage out of an Anthropic
    `/v1/messages?stream=true` response without buffering the whole body.

    Two events carry the numbers we want:

      event: message_start
      data: {"type":"message_start","message":{
               "model":"claude-opus-4-5-20260101",
               "usage":{"input_tokens":15,"cache_creation_input_tokens":0,
                        "cache_read_input_tokens":12000,"output_tokens":1}}}

      event: message_delta
      data: {"type":"message_delta","delta":{...},
             "usage":{"output_tokens":284}}

    `message_start` is the only place we see `input_tokens` and the
    cache counters; we snapshot them once. `message_delta` carries the
    CURRENT cumulative output_tokens (NOT a delta despite the event
    name) — keep updating to the latest value, the final one wins.

    We accumulate raw bytes into a small buffer and split on the SSE
    record separator (\\n\\n). Per-event parsing is cheap (one JSON
    decode of ≤1 KB) and gives us a robust event boundary even when
    a chunk lands mid-event."""

    # Cap buffer so a pathological upstream that NEVER emits \n\n can't
    # exhaust worker memory. 64 KB is far more than any real Anthropic
    # SSE event (typical event JSON is ~200–800 B); past that we drop
    # the head and let the scanner re-sync at the next \n\n.
    _MAX_BUF = 64 * 1024

    # Quick prefilter — avoids JSON-parsing every event when the SSE
    # stream is full of text deltas (95%+ of bytes). We only care about
    # events whose data line contains `"usage"`.
    _USAGE_HINT = re.compile(rb'"usage"\s*:')

    def __init__(self) -> None:
        self._buf = bytearray()
        self.model: str | None = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.cache_creation_tokens = 0
        self.cache_read_tokens = 0
        self.saw_any_usage = False

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buf.extend(chunk)
        while True:
            sep = self._buf.find(b"\n\n")
            if sep < 0:
                break
            event = bytes(self._buf[:sep])
            del self._buf[: sep + 2]
            self._parse_event(event)
        # Resync defense: if the buffer grew past the cap without ever
        # finding a record separator, drop everything except the last
        # _MAX_BUF/2 bytes so the next chunk can still complete an event.
        if len(self._buf) > self._MAX_BUF:
            del self._buf[: len(self._buf) - self._MAX_BUF // 2]

    def _parse_event(self, event: bytes) -> None:
        if not self._USAGE_HINT.search(event):
            return
        data_payload = bytearray()
        for line in event.split(b"\n"):
            if line.startswith(b"data:"):
                data_payload.extend(line[5:].lstrip())
                data_payload.extend(b"\n")
        if not data_payload:
            return
        try:
            data = json.loads(data_payload.decode("utf-8", "replace"))
        except json.JSONDecodeError:
            return
        if not isinstance(data, dict):
            return
        etype = data.get("type")
        if etype == "message_start":
            msg = data.get("message") or {}
            if isinstance(msg, dict):
                if isinstance(msg.get("model"), str):
                    self.model = msg["model"]
                usage = msg.get("usage")
                if isinstance(usage, dict):
                    self._merge_usage(usage)
        elif etype == "message_delta":
            usage = data.get("usage")
            if isinstance(usage, dict):
                # message_delta carries CUMULATIVE output_tokens (latest
                # wins); other fields shouldn't appear here but we
                # tolerate them defensively.
                self._merge_usage(usage)

    def _merge_usage(self, usage: dict) -> None:
        def _take(key: str, current: int) -> int:
            v = usage.get(key)
            if not isinstance(v, (int, float)):
                return current
            return int(v)

        self.input_tokens          = _take("input_tokens",                self.input_tokens)
        self.output_tokens         = _take("output_tokens",               self.output_tokens)
        self.cache_creation_tokens = _take("cache_creation_input_tokens", self.cache_creation_tokens)
        self.cache_read_tokens     = _take("cache_read_input_tokens",     self.cache_read_tokens)
        self.saw_any_usage = True

    def snapshot(self) -> dict | None:
        """Return the final accumulated usage as a plain dict, or None
        if we never saw a usage event (e.g. upstream error before
        message_start). The dict matches the IPC `usage` event shape:
        keys are the wire-stable identifiers expected by UsageStore."""
        if not self.saw_any_usage:
            return None
        return {
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_creation_tokens": self.cache_creation_tokens,
            "cache_read_tokens": self.cache_read_tokens,
        }


class _FlowState:
    """Per-hijacked-flow state shared between the request hook (which
    arms the stall watchdog the moment hijack completes), the
    responseheaders hook (which mounts a stream tap that resets the
    watchdog on each chunk), and the error hook (which closes the
    channel on upstream connection failure).

    Holding state per flow.id (rather than a single self._ slot) keeps
    a stale flow that times out late from clobbering a fresh hijack
    that already took over self._active_flow_id."""

    def __init__(self, addon: "HijackAddon", flow_id: str,
                 channel, stall_timeout: float) -> None:
        self.addon = addon
        self.flow_id = flow_id
        self.flow_id_short = flow_id[:8]
        self.channel = channel
        self.user = addon.session.user_id
        self.stall_timeout = stall_timeout
        self.loop = asyncio.get_running_loop()
        self.watchdog: asyncio.TimerHandle | None = None
        self.closed = False
        # Token-usage scanner — owns a small buffer of unsplit SSE bytes
        # and snapshots model + token counts as message_start / message_delta
        # events fly past. snapshot() is dumped onto channel.usage at
        # end-of-stream so worker._handle can emit a usage IPC.
        self.usage_scanner = _UsageScanner()

    def arm(self) -> None:
        if self.watchdog is not None:
            self.watchdog.cancel()
        self.watchdog = self.loop.call_later(self.stall_timeout, self._fire)

    def _fire(self) -> None:
        if self.closed:
            return
        log.warning("user=%s response went silent for %.0fs (no chunk from "
                    "upstream); closing channel flow=%s",
                    self.user, self.stall_timeout, self.flow_id_short)
        self.close("force-closed by stall watchdog")

    def close(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        if self.watchdog is not None:
            self.watchdog.cancel()
            self.watchdog = None
        if self.channel is not None:
            try:
                self.channel.queue.put_nowait(None)
            except Exception:
                pass
        # Drop from addon's per-flow dict so we don't leak entries
        # forever (a stuck request that never sees responseheaders
        # would otherwise sit in the dict for the worker's whole life).
        self.addon._flow_states.pop(self.flow_id, None)
        # "complete" fires once per request — DEBUG. Force-close /
        # upstream-error reasons stay at INFO so an unhealthy worker
        # produces a visible audit trail without normal traffic
        # drowning out the signal.
        if reason == "complete":
            log.debug("user=%s response stream complete flow=%s",
                      self.user, self.flow_id_short)
        else:
            log.info("user=%s response stream %s flow=%s",
                     self.user, reason, self.flow_id_short)


class HijackAddon:
    """One instance per ClaudeSession (and per mitm listener port).
    Holds a back-reference to its session to read the pending request
    body and write streaming response chunks."""

    def __init__(self, session: "ClaudeSession") -> None:
        self.session = session
        # the flow currently being streamed back to the API client (if any)
        self._active_flow_id: str | None = None
        # Per-flow watchdog/channel state, keyed by mitm flow.id. An
        # entry is added when the request hook hijacks the flow and
        # removed by _FlowState.close (normal EOS, watchdog fire, or
        # upstream error).
        self._flow_states: dict[str, _FlowState] = {}

    # ---- request hook: swap body ----

    def request(self, flow: http.HTTPFlow) -> None:
        if flow.request.host not in ANTHROPIC_HOSTS:
            return
        # DEBUG: claude CLI makes 6+ sibling calls per request (eval,
        # grove, mcp-registry, etc.) — at INFO this was the loudest
        # source of per-request noise in docker logs. The hijack line
        # below carries the one fact an operator actually needs.
        log.debug("user=%s saw outbound %s %s",
                  self.session.user_id,
                  flow.request.method, flow.request.path)
        # flow.request.path looks like "/v1/messages?beta=true" — strip
        # the query before matching, then accept any sub-resource that
        # starts with /v1/messages (count_tokens, etc., never see hijack
        # because of pending-slot gating).
        bare_path = flow.request.path.split("?", 1)[0]
        if bare_path != MESSAGES_PATH_PREFIX:
            return

        pending = self.session.pending
        if pending is None:
            # No user request waiting — let claude's own (placeholder) call go
            # through. We can't easily cancel it without making claude unhappy,
            # so let it run and just don't forward the response.
            log.debug("user=%s no pending request, passing through",
                      self.session.user_id)
            return

        merged = self._merge_body(flow.request.get_text() or "{}", pending.body)
        # Force streaming on so we can pipe SSE back to the API caller.
        merged["stream"] = True
        flow.request.set_text(json.dumps(merged))
        flow.request.headers["content-type"] = "application/json"
        # Disable gzip on the SSE response: we're going to forward the
        # bytes to a downstream API client that didn't necessarily ask
        # for gzip, and decompressing streamed gzip in our `_tap` adds
        # complexity for no gain. Accept-Encoding is a transport hint —
        # not part of the cc_entrypoint billing fingerprint.
        flow.request.headers["accept-encoding"] = "identity"
        # Strip the 1M-context beta from `anthropic-beta`. claude code
        # advertises `context-1m-...` unconditionally; on a subscription
        # account that makes Anthropic classify the call as a premium
        # "long context" request requiring pay-as-you-go usage credits
        # ("Usage credits are required for long context requests"). Our
        # callers send small bodies, so drop just that token — the
        # identity-bearing betas (`claude-code-...`, `oauth-...`) stay.
        beta = flow.request.headers.get("anthropic-beta")
        if beta:
            kept = [t.strip() for t in beta.split(",")
                    if t.strip() and not t.strip().startswith(STRIP_BETA_PREFIX)]
            new_beta = ",".join(kept)
            if new_beta != beta:
                flow.request.headers["anthropic-beta"] = new_beta
                # DEBUG: fires every request; the strip is invariant
                # behavior, an operator never needs to watch it.
                log.debug("user=%s stripped %s* from anthropic-beta",
                          self.session.user_id, STRIP_BETA_PREFIX)
        # mitmproxy will recompute content-length. We deliberately do NOT
        # mutate other identity-bearing headers — claude's User-Agent /
        # x-app / x-stainless-* must reach Anthropic verbatim for the call
        # to be bucketed into subscription quota.

        self._active_flow_id = flow.id
        pending.consumed.set()
        # Clear the slot so subsequent outbound calls from claude don't get
        # hijacked by this same pending request.
        self.session.pending = None

        # Arm the stall watchdog NOW (at hijack time, not at
        # responseheaders time) so an upstream that accepts the request
        # but never sends response headers at all — a silent hang
        # before the first byte — is still recoverable. The earlier
        # version armed only in responseheaders, which left this gap:
        # in production we saw worker stuck with in_flight=1, bytes=0,
        # stalled=215s, because Anthropic's TCP connection was open
        # but no HTTP response had ever come back, so responseheaders
        # never fired, so the watchdog never armed.
        stall_timeout = self.session.response_stall_timeout
        state = _FlowState(
            addon=self,
            flow_id=flow.id,
            channel=self.session.response,
            stall_timeout=stall_timeout,
        )
        state.arm()
        self._flow_states[flow.id] = state

        # DEBUG: per-request. The hijack itself is invariant — what an
        # operator cares about (failures, force-closes, rate-limits) is
        # logged at WARNING by the watchdog / 429 paths.
        log.debug("user=%s hijacked outbound /v1/messages flow=%s model=%s",
                  self.session.user_id, flow.id[:8], merged.get("model"))

    def _merge_body(self, original_text: str, user_body: dict) -> dict:
        """Whitelist-merge: start from claude's original body (keeps tools,
        metadata, anthropic_version, etc.), then overlay user-owned fields.

        `system` gets special treatment: if the user supplied one, we keep
        claude's first system block (which carries the
        `x-anthropic-billing-header: cc_entrypoint=cli` billing fingerprint)
        and append the user's system after it as a new text block with
        cache_control so the user's stable system text can be reused across
        calls. claude's other system blocks (Claude Code persona +
        instructions) are dropped so they don't conflict with the user's."""
        try:
            base = json.loads(original_text) if original_text else {}
            if not isinstance(base, dict):
                base = {}
        except json.JSONDecodeError:
            base = {}

        merged = dict(base)
        for k in USER_OWNED_BODY_FIELDS:
            if k in user_body:
                merged[k] = user_body[k]

        if "system" in user_body:
            merged["system"] = self._merge_system(
                base.get("system"), user_body["system"])

        # Pass through other fields the user set that claude's base doesn't
        # carry (e.g. unusual top-level keys). Never overwrite identity
        # fields like `metadata` / `tools` — to opt those into user control
        # would require explicit additions to USER_OWNED_BODY_FIELDS.
        for k, v in user_body.items():
            if k in USER_OWNED_BODY_FIELDS or k == "system":
                continue
            if k not in merged:
                merged[k] = v

        # Model-coupled generation knobs. The claude CLI tunes these for
        # the model IT runs (e.g. output_config={"effort":"xhigh"} is an
        # Opus-tier level; sonnet rejects "xhigh"). When the API caller
        # overrode `model` to a different one, drop the CLI's versions so
        # the new model falls back to its own defaults. Kept verbatim when
        # the caller's model matches the CLI's, or when the caller set the
        # field explicitly.
        if merged.get("model") != base.get("model"):
            for k in ("output_config", "thinking", "context_management"):
                if k in merged and k not in user_body:
                    dropped = merged.pop(k)
                    log.debug("user=%s dropped CLI %s=%s (model %s != CLI %s)",
                              self.session.user_id, k, json.dumps(dropped),
                              merged.get("model"), base.get("model"))

        # Coerce legacy effort tier. Anthropic's current API rejects
        # output_config.effort="xhigh" with:
        #   This model does not support effort level 'xhigh'.
        #   Supported levels: high, low, max, medium.
        # claude CLI 2.1.x still defaults to xhigh; when the caller's
        # model matches the CLI's the model-mismatch block above leaves
        # output_config in place and the request 400s. Translate to
        # "high" so the call always lands.
        oc = merged.get("output_config")
        if isinstance(oc, dict) and oc.get("effort") == "xhigh":
            oc = dict(oc)
            oc["effort"] = "high"
            merged["output_config"] = oc
            log.debug("user=%s normalized output_config.effort xhigh→high "
                      "(upstream rejects xhigh)", self.session.user_id)

        # Anthropic rejects `thinking` when `tool_choice` forces a tool call.
        # Exact error: "Thinking may not be enabled when tool_choice forces
        # tool use." This combo shows up routinely in Claude Code's sub-agent
        # / Task delegation paths (forced `{"type":"tool","name":"Task"}`)
        # because the CLI side enables thinking by default on Opus 4.x.
        # Drop thinking — the request is delegation-style where the model
        # picks a tool to run, reasoning offers little vs. the cost of a 400.
        tc = merged.get("tool_choice")
        if (isinstance(tc, dict)
                and tc.get("type") in ("tool", "any")
                and "thinking" in merged):
            dropped = merged.pop("thinking")
            log.debug("user=%s dropped thinking=%s (tool_choice.type=%r forces "
                      "tool use, upstream rejects the combo)",
                      self.session.user_id, json.dumps(dropped), tc.get("type"))
        return merged

    @staticmethod
    def _merge_system(base_system, user_system) -> list:
        """Build a new system list: [claude billing header block,
        user system block]. Returns a list of blocks (Anthropic system
        prompts are always list-of-blocks when sent to /v1/messages with
        beta=true)."""
        billing_block = None
        if isinstance(base_system, list) and base_system:
            billing_block = base_system[0]
        # We don't try to detect the billing block heuristically — claude
        # code 2.1.x always puts it at system[0]. If a future version
        # changes that, the merge would drop the billing header and
        # billing would route to the SDK quota. Fail loud rather than
        # silently degrade by logging when system[0] doesn't look like
        # the billing header.
        if billing_block is not None and isinstance(billing_block, dict):
            text = billing_block.get("text", "")
            if "cc_entrypoint" not in text:
                log.warning("claude system[0] missing cc_entrypoint billing "
                            "header — billing may not route to subscription")

        if isinstance(user_system, str):
            user_blocks = [{
                "type": "text",
                "text": user_system,
                # Allow Anthropic to cache the user's (typically stable)
                # system text across calls.
                "cache_control": {"type": "ephemeral"},
            }] if user_system else []
        elif isinstance(user_system, list):
            user_blocks = list(user_system)
        else:
            user_blocks = []

        out = []
        if billing_block is not None:
            out.append(billing_block)
        out.extend(user_blocks)
        return out

    # ---- response streaming ----

    def responseheaders(self, flow: http.HTTPFlow) -> None:
        """Install the stream tap that mirrors each upstream chunk into
        our channel and resets the watchdog. The watchdog itself was
        already armed by the request hook at hijack time; this hook
        just hands it the per-chunk reset signal once data starts
        flowing. mitmproxy 11 contract for `flow.response.stream`:

          - Set to True for pass-through streaming.
          - Set to a callable `(data: bytes) -> bytes | list[bytes]`
            to transform each chunk. An empty `b""` argument signals
            end-of-stream.
        """
        if flow.id != self._active_flow_id:
            return
        state = self._flow_states.get(flow.id)
        if state is None:
            # request hook didn't hijack this flow (e.g., not a /v1/messages
            # path, or hijacked then a stale entry was already cleared).
            # Pass-through with no tapping.
            return

        # 429 is where Anthropic puts the precise rate-limit reset info,
        # and it's usually in HEADERS, not the body. If we wait for the
        # body scan we get a generic "rate_limit_error" but lose the
        # exact `anthropic-ratelimit-*-reset` / `retry-after` numeric
        # value. Snapshot it here while we still have the response
        # object, dispatch as a session-wide event so the main process
        # can mark the account with the precise epoch.
        if flow.response.status_code == 429:
            self._emit_429_event(flow)

        def _tap(data: bytes) -> bytes:
            if data:
                if state.channel is not None and not state.closed:
                    try:
                        state.channel.queue.put_nowait(bytes(data))
                    except Exception:
                        log.exception("user=%s failed pushing chunk", state.user)
                # Side-tap into the usage scanner. Cheap pre-filter
                # inside the scanner skips JSON-parsing the common case
                # (text_delta chunks have no `"usage"` substring).
                try:
                    state.usage_scanner.feed(data)
                except Exception:
                    log.exception("user=%s usage scan failed", state.user)
                state.arm()  # reset stall watchdog on each chunk
                return data
            # data == b"" → end-of-stream marker from mitm. Snapshot
            # usage onto the channel BEFORE closing so worker._handle
            # picks it up while iterating; the channel itself is the
            # rendezvous between mitm (writer) and worker (reader).
            if state.channel is not None:
                snap = state.usage_scanner.snapshot()
                if snap is not None:
                    state.channel.usage = snap
            state.close("complete")
            return b""

        flow.response.stream = _tap

    def _emit_429_event(self, flow: http.HTTPFlow) -> None:
        """Extract the rate-limit reset moment from a 429 response and
        push a `rate_limit` event onto session.events. The body scan
        path (session._maybe_detect_rate_limit) is a fallback for when
        the headers don't carry timing info; in our experience
        Anthropic's headers always do, and they're more precise than
        the body text."""
        import time as _time
        headers = flow.response.headers
        until_epoch: float | None = None
        reason = "rate_limit"

        # Anthropic's unified rate-limit headers (as of CLI 2.1.x):
        #   anthropic-ratelimit-unified-5h-reset
        #   anthropic-ratelimit-unified-weekly-reset
        #   anthropic-ratelimit-requests-reset (RFC 3339 timestamp)
        #   anthropic-ratelimit-tokens-reset   (RFC 3339 timestamp)
        # Collect every reset header that looks plausible — we'll pick
        # the LATEST timestamp (account is blocked until ALL limits
        # clear) and classify the reason from the WINDOW SIZE rather
        # than the header name. The latter is unreliable: a "5h-reset"
        # header can carry a 4-day value when the operator has hit the
        # weekly limit and the 5h header is just echoing the longer
        # blocker.
        candidates: list[tuple[str, float]] = []
        for hname, hval in list(headers.items()):
            ln = hname.lower()
            if not ln.startswith("anthropic-ratelimit-") and ln != "retry-after":
                continue
            if not ln.endswith("reset") and ln != "retry-after":
                continue
            v = hval.strip()
            ts: float | None = None
            # Try integer (epoch seconds, or relative for retry-after)
            try:
                n = int(v)
                if n >= 1_500_000_000:
                    ts = float(n)                          # absolute epoch
                elif ln == "retry-after":
                    ts = _time.time() + float(n)           # relative
                else:
                    ts = _time.time() + float(n)
            except ValueError:
                # Not an integer — try ISO 8601
                try:
                    import datetime as _dt
                    raw = v[:-1] if v.endswith("Z") else v
                    parsed = _dt.datetime.fromisoformat(raw)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
                    ts = parsed.timestamp()
                except ValueError:
                    continue
            if ts is None:
                continue
            candidates.append((ln, ts))

        if candidates:
            until_epoch = max(ts for _, ts in candidates)
            from ..rate_limit import classify_rate_limit_reason
            reason = classify_rate_limit_reason(until_epoch - _time.time())
            # DEBUG: the resulting account mark is logged at WARNING
            # by SessionManager._mark_account_issue ("account=X
            # rate_limit (weekly_limit); routing will skip..."), which
            # is what operators actually need. Keep the per-header
            # details available for `LOG_LEVEL=DEBUG` triage.
            log.debug("user=%s 429 with headers: %s; using until_epoch=%d "
                      "reason=%s", self.session.user_id,
                      [(n, int(ts)) for n, ts in candidates],
                      int(until_epoch), reason)
        else:
            log.warning("user=%s 429 but no recognisable reset header; "
                        "body-scan fallback will mark with default window",
                        self.session.user_id)
            return

        # Push the event for worker.amain's relay to pipe up as IPC.
        try:
            self.session.events.put_nowait({
                "type": "rate_limit",
                "until_epoch": float(until_epoch),
                "reason": reason,
                "source": "http_429_headers",
            })
        except Exception:
            log.exception("failed to push 429 event")

    # ---- /api/oauth/usage capture ----

    def response(self, flow: http.HTTPFlow) -> None:
        """Capture the JSON body of `GET /api/oauth/usage` and ship it
        to the main process via session.events. The `/v1/messages` path
        is handled streamingly in responseheaders (above); /api/oauth/usage
        is small enough to let mitm buffer the full body, so we read
        flow.response.content here and don't need stream-tapping."""
        if flow.request.host not in ANTHROPIC_HOSTS:
            return
        bare_path = flow.request.path.split("?", 1)[0]
        if bare_path != QUOTA_USAGE_PATH:
            return
        status = flow.response.status_code if flow.response is not None else None
        if status == 429:
            # Upstream rate-limits /api/oauth/usage strictly (observed
            # retry-after: 3600 — 1 call/hour/account in practice).
            # Surface retry-after so QuotaProbeService can skip probes
            # during the cooldown instead of burning another /usage
            # PTY interrupt every tick.
            retry_after_raw = flow.response.headers.get("retry-after") if flow.response else None
            retry_after: float | None = None
            if retry_after_raw:
                try:
                    retry_after = float(retry_after_raw.strip())
                except ValueError:
                    retry_after = None
            import time as _time
            try:
                self.session.events.put_nowait({
                    "type": "quota_usage_429",
                    "retry_after_seconds": retry_after,
                    "fetched_at_unix": _time.time(),
                })
            except Exception:
                log.exception("user=%s failed to push quota_usage_429 event",
                              self.session.user_id)
            log.warning("user=%s /api/oauth/usage 429 retry_after=%s",
                        self.session.user_id, retry_after)
            return
        if status != 200:
            log.warning("user=%s /api/oauth/usage returned HTTP %s; skipping",
                        self.session.user_id, status)
            return
        try:
            text = flow.response.get_text() or "{}"
            body = json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning("user=%s /api/oauth/usage body parse failed: %s",
                        self.session.user_id, e)
            return
        import time as _time
        try:
            self.session.events.put_nowait({
                "type": "quota_usage",
                "body": body,
                "fetched_at_unix": _time.time(),
            })
            log.debug("user=%s captured /api/oauth/usage response",
                      self.session.user_id)
        except Exception:
            log.exception("user=%s failed to push quota_usage event",
                          self.session.user_id)

    def error(self, flow: http.HTTPFlow) -> None:
        """Mitm hit a flow-level error — typically upstream connection
        reset, TLS handshake failure, or server hangup before sending
        any response. _tap will never fire (no body stream) so close
        the channel directly so the worker's _handle unblocks instead
        of waiting for the watchdog timeout."""
        state = self._flow_states.get(flow.id)
        if state is None:
            return
        err = getattr(flow, "error", None)
        log.warning("user=%s upstream flow error: %s flow=%s",
                    state.user, err, state.flow_id_short)
        state.close("upstream error")
