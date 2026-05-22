from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from mitmproxy import http

if TYPE_CHECKING:
    from ..session.session import ClaudeSession

log = logging.getLogger(__name__)

ANTHROPIC_HOSTS = {"api.anthropic.com"}
# Real claude code hits /v1/messages?beta=true — flow.request.path
# carries the query string, so we match on the bare path prefix.
MESSAGES_PATH_PREFIX = "/v1/messages"

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
        log.info("user=%s saw outbound %s %s",
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
                log.info("user=%s stripped %s* from anthropic-beta",
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
        stall_timeout = getattr(
            self.session, "response_stall_timeout", 90.0)
        state = _FlowState(
            addon=self,
            flow_id=flow.id,
            channel=self.session.response,
            stall_timeout=stall_timeout,
        )
        state.arm()
        self._flow_states[flow.id] = state

        log.info("user=%s hijacked outbound /v1/messages flow=%s model=%s",
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
                    log.info("user=%s dropped CLI %s=%s (model %s != CLI %s)",
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
            log.info("user=%s normalized output_config.effort xhigh→high "
                     "(upstream rejects xhigh)", self.session.user_id)
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

        def _tap(data: bytes) -> bytes:
            if data:
                if state.channel is not None and not state.closed:
                    try:
                        state.channel.queue.put_nowait(bytes(data))
                    except Exception:
                        log.exception("user=%s failed pushing chunk", state.user)
                state.arm()  # reset stall watchdog on each chunk
                return data
            # data == b"" → end-of-stream marker from mitm
            state.close("complete")
            return b""

        flow.response.stream = _tap

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
