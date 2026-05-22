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


class HijackAddon:
    """One instance per ClaudeSession (and per mitm listener port).
    Holds a back-reference to its session to read the pending request
    body and write streaming response chunks."""

    def __init__(self, session: "ClaudeSession") -> None:
        self.session = session
        # the flow currently being streamed back to the API client (if any)
        self._active_flow_id: str | None = None

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
        if flow.id != self._active_flow_id:
            return
        # mitmproxy 11 contract for `flow.response.stream`:
        #   - Set to True for pass-through streaming.
        #   - Set to a callable `(data: bytes) -> bytes | list[bytes]`
        #     to transform each chunk. An empty `b""` argument signals
        #     end-of-stream (after which the callable may also return
        #     bytes to append).
        # We use the callable form so we can mirror each chunk into the
        # user's ResponseChannel while still letting it flow to claude.
        channel = self.session.response
        user = self.session.user_id
        flow_id_short = flow.id[:8]

        # Stall watchdog. Recovers from the pattern where Anthropic
        # returns a small non-SSE error body (~150 bytes for a
        # rate_limit_error JSON) then leaves the connection idle
        # without a proper end-of-stream signal. Without this, _tap
        # never sees b"", the channel never gets a None sentinel, the
        # worker's _handle stays in `async for chunk in channel.iter()`
        # forever, and the worker looks busy on /status indefinitely.
        # The watchdog re-arms on every chunk; if no chunk arrives for
        # the configured window we synthesise the EOS ourselves.
        stall_timeout = getattr(self.session, "response_stall_timeout", 90.0)
        loop = asyncio.get_running_loop()
        # Mutable single-slot holder so the nested closures can swap
        # the TimerHandle without `nonlocal` gymnastics.
        watchdog: list = [None]
        closed = [False]

        def _close_channel(reason: str) -> None:
            if closed[0]:
                return
            closed[0] = True
            if watchdog[0] is not None:
                watchdog[0].cancel()
                watchdog[0] = None
            if channel is not None:
                try:
                    channel.queue.put_nowait(None)
                except Exception:
                    pass
            log.info("user=%s response stream %s flow=%s",
                     user, reason, flow_id_short)

        def _fire_watchdog() -> None:
            log.warning("user=%s response went silent for %.0fs (no chunk "
                        "from upstream); closing channel to recover from "
                        "stuck non-SSE response or upstream hang flow=%s",
                        user, stall_timeout, flow_id_short)
            _close_channel("force-closed by stall watchdog")

        def _arm_watchdog() -> None:
            if watchdog[0] is not None:
                watchdog[0].cancel()
            watchdog[0] = loop.call_later(stall_timeout, _fire_watchdog)

        def _tap(data: bytes) -> bytes:
            if data:
                if channel is not None and not closed[0]:
                    try:
                        channel.queue.put_nowait(bytes(data))
                    except Exception:
                        log.exception("user=%s failed pushing chunk", user)
                _arm_watchdog()
                return data
            # data == b"" → end-of-stream marker from mitm
            _close_channel("complete")
            return b""

        # Arm immediately so a response that yields ZERO chunks (e.g.
        # mitm sets up the stream then the connection silently drops
        # before any body bytes arrive) still gets recovered.
        _arm_watchdog()
        flow.response.stream = _tap
