from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ..session.manager import SessionManager

log = logging.getLogger(__name__)


def _extract_litellm_headers(headers) -> dict[str, str]:
    """Pick up x-litellm-* headers forwarded by LiteLLM Proxy when
    `add_user_information_to_llm_headers: true` is set in its config.
    These carry the original virtual-key user identity (user_id,
    org_id, team_id, etc.) which is otherwise invisible to us since
    LiteLLM authenticates upstream with its own configured api_key.

    Returns a dict keyed by the suffix in snake_case
    (`x-litellm-user-id` → `user_id`), or an empty dict if no such
    headers are present. Stored under `body_summary.litellm` so it
    shows up on /status and the /ui dashboard."""
    out: dict[str, str] = {}
    for k, v in headers.items():
        kl = k.lower()
        if kl.startswith("x-litellm-"):
            key = kl[len("x-litellm-"):].replace("-", "_")
            if key:
                out[key] = v
    return out


def build_router(manager: SessionManager, auth_dep) -> APIRouter:
    router = APIRouter()

    @router.post("/v1/messages")
    async def messages(req: Request, pool: list[str] = Depends(auth_dep)) -> Any:
        body = await req.json()
        wants_stream = bool(body.get("stream", False))
        litellm = _extract_litellm_headers(req.headers)
        request_metadata = {"litellm": litellm} if litellm else None

        sess = await manager.pick(pool)
        channel = await sess.call(body, request_metadata=request_metadata)

        if wants_stream:
            async def gen():
                """Forward upstream SSE bytes verbatim. But if upstream
                degenerates into one of three shapes:
                  (1) zero bytes ever (mitm intercept timeout, watchdog
                      fired before responseheaders, etc.)
                  (2) a plain JSON 4xx error body (429 / 401 from
                      Anthropic — they don't always wrap these in SSE)
                  (3) SSE that's cut off before message_stop
                a strict client like Claude Code crashes on
                `usage.input_tokens` because `usage` is undefined. So we
                sniff the first non-whitespace byte and synthesize a
                complete, valid Anthropic SSE sequence (with empty usage
                + the upstream error text as the assistant message)
                whenever we detect (1) or (2). For (3) we just append a
                synthetic message_stop so clients don't hang."""
                head = bytearray()
                decided = False        # have we figured out SSE vs JSON yet?
                is_sse = False
                saw_message_stop = False
                model_hint = (body.get("model") or "") if isinstance(body, dict) else ""

                async for chunk in channel.iter():
                    if not decided:
                        head.extend(chunk)
                        stripped = bytes(head).lstrip()
                        if not stripped:
                            continue                # still only whitespace
                        first = stripped[:1]
                        if first in (b"e", b"d", b":"):
                            # SSE: starts with "event:", "data:", or comment ":"
                            is_sse = True
                            decided = True
                            if b"event: message_stop" in head:
                                saw_message_stop = True
                            yield bytes(head); head.clear()
                            continue
                        if first == b"{":
                            # Non-SSE JSON body — keep buffering till EOF, then synthesize
                            continue
                        # Unknown prefix → fall back to passthrough
                        is_sse = True
                        decided = True
                        yield bytes(head); head.clear()
                        continue

                    if is_sse:
                        if b"event: message_stop" in chunk:
                            saw_message_stop = True
                        yield chunk
                    else:
                        head.extend(chunk)

                # Channel closed. Decide synthesis.
                if not decided:
                    if head.strip():
                        # Pure JSON body — parse and surface as synthetic SSE error
                        text = bytes(head).decode("utf-8", "replace")
                        err_type, err_msg = "error", text[:500]
                        try:
                            parsed = json.loads(text)
                            err = parsed.get("error") if isinstance(parsed, dict) else None
                            if isinstance(err, dict):
                                err_type = err.get("type", "error")
                                err_msg = err.get("message", text[:500])
                        except json.JSONDecodeError:
                            pass
                        log.warning("synthesizing SSE error from non-SSE upstream body: %s",
                                    text[:200])
                        yield _synthetic_error_sse(err_type, err_msg, model_hint)
                    else:
                        # Channel closed with zero bytes — mitm intercept timeout, etc.
                        log.warning("synthesizing SSE error: upstream channel closed without any bytes")
                        yield _synthetic_error_sse(
                            "upstream_unavailable",
                            "上游 channel 在收到任何字节前已关闭（mitm intercept 超时 / "
                            "PTY 卡 modal / watchdog 早期触发）。请稍后重试，或在 "
                            "/ui dashboard 检查 worker 状态。",
                            model_hint)
                elif is_sse and not saw_message_stop:
                    # SSE was cut off mid-stream (watchdog close, etc.).
                    # Inject a final message_stop so the client side state
                    # machine completes cleanly instead of hanging.
                    log.warning("appending synthetic message_stop to truncated upstream stream")
                    yield (b'event: message_delta\n'
                           b'data: {"type":"message_delta",'
                           b'"delta":{"stop_reason":"error","stop_sequence":null},'
                           b'"usage":{"output_tokens":0}}\n\n'
                           b'event: message_stop\n'
                           b'data: {"type":"message_stop"}\n\n')
            return StreamingResponse(gen(), media_type="text/event-stream")

        # Buffer + collapse SSE events into a single Anthropic non-streaming
        # response. The simplest faithful conversion is to reconstruct the
        # final Message from the event stream.
        message = await _collapse_stream(channel)
        return JSONResponse(message)

    return router


def _synthetic_error_sse(err_type: str, err_msg: str, model: str = "") -> bytes:
    """Build a complete, schema-valid Anthropic SSE event sequence that
    represents an error. Includes a populated `usage` object on
    message_start so strict clients (Claude Code) that index
    `data.usage.input_tokens` without a guard don't crash with
    "Cannot read properties of undefined (reading 'input_tokens')".

    Used by the streaming /v1/messages handler whenever the upstream
    response degenerates into zero bytes or a non-SSE JSON error body,
    so we never expose those raw shapes to downstream API clients."""
    def evt(event_type: str, payload: dict) -> bytes:
        return (f"event: {event_type}\ndata: "
                + json.dumps(payload, ensure_ascii=False) + "\n\n").encode()

    msg_id = "msg_synthetic_error"
    body_text = f"[upstream {err_type}] {err_msg}"
    return b"".join([
        evt("message_start", {
            "type": "message_start",
            "message": {
                "id": msg_id, "type": "message", "role": "assistant",
                "content": [], "model": model,
                "stop_reason": None, "stop_sequence": None,
                # 0/0 usage so input_tokens access is well-defined.
                "usage": {
                    "input_tokens": 0, "output_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                },
            },
        }),
        evt("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""},
        }),
        evt("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": body_text},
        }),
        evt("content_block_stop", {"type": "content_block_stop", "index": 0}),
        evt("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "error", "stop_sequence": None},
            "usage": {"output_tokens": 0},
        }),
        evt("message_stop", {"type": "message_stop"}),
    ])


async def _collapse_stream(channel) -> dict[str, Any]:
    """Re-assemble Anthropic SSE events into a final Message JSON object,
    matching the non-streaming /v1/messages response shape."""
    message: dict[str, Any] = {}
    content_blocks: list[dict[str, Any]] = []
    buf = b""
    saw_sse_event = False
    async for chunk in channel.iter():
        buf += chunk
        while b"\n\n" in buf:
            raw_event, buf = buf.split(b"\n\n", 1)
            event = _parse_sse_event(raw_event)
            if not event:
                continue
            saw_sse_event = True
            etype = event.get("event")
            data = event.get("data") or {}
            if etype == "message_start":
                message = dict(data.get("message", {}))
                content_blocks = []
            elif etype == "content_block_start":
                idx = data.get("index", len(content_blocks))
                block = dict(data.get("content_block", {}))
                while len(content_blocks) <= idx:
                    content_blocks.append({})
                content_blocks[idx] = block
            elif etype == "content_block_delta":
                idx = data.get("index", 0)
                delta = data.get("delta", {})
                if idx >= len(content_blocks):
                    continue
                block = content_blocks[idx]
                dtype = delta.get("type")
                if dtype == "text_delta":
                    block["text"] = block.get("text", "") + delta.get("text", "")
                elif dtype == "input_json_delta":
                    block["partial_json"] = block.get("partial_json", "") + delta.get("partial_json", "")
            elif etype == "content_block_stop":
                idx = data.get("index", 0)
                if idx < len(content_blocks):
                    block = content_blocks[idx]
                    if "partial_json" in block:
                        try:
                            block["input"] = json.loads(block.pop("partial_json"))
                        except json.JSONDecodeError:
                            block["input"] = {}
            elif etype == "message_delta":
                delta = data.get("delta", {})
                for k, v in delta.items():
                    message[k] = v
                usage = data.get("usage")
                if usage:
                    message.setdefault("usage", {}).update(usage)
            elif etype == "message_stop":
                pass
            elif etype == "error":
                log.warning("upstream Anthropic SSE error event: %s", data)
                err = data.get("error") if isinstance(data, dict) else None
                err_type = (err or {}).get("type", "error") if isinstance(err, dict) else "error"
                err_msg = (err or {}).get("message", str(data)) if isinstance(err, dict) else str(data)
                message.setdefault("id", "")
                message.setdefault("type", "message")
                message.setdefault("role", "assistant")
                message.setdefault("model", "")
                message.setdefault("usage", {})
                message["stop_reason"] = "error"
                message["stop_sequence"] = None
                content_blocks = [{
                    "type": "text",
                    "text": f"[upstream {err_type}] {err_msg}",
                }]
    # Upstream returned a non-SSE body (typically a 4xx JSON error like
    # {"type":"error","error":{...}}). Surface it as stop_reason=error
    # with the error text in content so the caller can see what happened
    # instead of getting empty content + stop_reason=end_turn.
    if not saw_sse_event and buf.strip():
        leftover = buf.strip()
        body: Any = None
        try:
            body = json.loads(leftover.decode("utf-8", "replace"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = None
        if isinstance(body, dict) and body.get("type") == "error":
            err = body.get("error") if isinstance(body.get("error"), dict) else {}
            err_type = (err or {}).get("type", "error")
            err_msg = (err or {}).get("message", json.dumps(body))
            log.warning("upstream Anthropic non-SSE error body: %s", body)
            message.setdefault("id", body.get("request_id", ""))
            message.setdefault("model", "")
            message.setdefault("usage", {})
            message["stop_reason"] = "error"
            message["stop_sequence"] = None
            content_blocks = [{
                "type": "text",
                "text": f"[upstream {err_type}] {err_msg}",
            }]
        else:
            snippet = leftover[:500].decode("utf-8", "replace")
            log.warning("upstream returned non-SSE body (no events parsed): %r",
                        snippet)
            message["stop_reason"] = "error"
            message["stop_sequence"] = None
            content_blocks = [{
                "type": "text",
                "text": f"[upstream non-SSE response] {snippet}",
            }]

    message["content"] = content_blocks
    message.setdefault("stop_reason", "end_turn")
    message.setdefault("stop_sequence", None)
    message.setdefault("type", "message")
    message.setdefault("role", "assistant")
    # Ensure usage always has the four fields Claude Code expects to be
    # numeric. Without this, the error branches above leave usage = {}
    # and CC crashes on `usage.input_tokens`. We overlay any real usage
    # values on top of zeros so genuine numbers are preserved.
    actual_usage = message.get("usage") or {}
    message["usage"] = {
        "input_tokens": 0, "output_tokens": 0,
        "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0,
        **actual_usage,
    }
    return message


def _parse_sse_event(raw: bytes) -> dict[str, Any] | None:
    event_type = None
    data_lines: list[str] = []
    for line in raw.split(b"\n"):
        line = line.strip()
        if not line:
            continue
        if line.startswith(b"event:"):
            event_type = line[6:].strip().decode()
        elif line.startswith(b"data:"):
            data_lines.append(line[5:].strip().decode())
    if event_type is None and not data_lines:
        return None
    data_raw = "\n".join(data_lines)
    try:
        data = json.loads(data_raw) if data_raw else {}
    except json.JSONDecodeError:
        data = {}
    return {"event": event_type, "data": data}
