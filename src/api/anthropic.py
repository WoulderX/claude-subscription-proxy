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
                async for chunk in channel.iter():
                    yield chunk
            return StreamingResponse(gen(), media_type="text/event-stream")

        # Buffer + collapse SSE events into a single Anthropic non-streaming
        # response. The simplest faithful conversion is to reconstruct the
        # final Message from the event stream.
        message = await _collapse_stream(channel)
        return JSONResponse(message)

    return router


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
