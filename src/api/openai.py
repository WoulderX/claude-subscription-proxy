from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ..session.manager import SessionManager
from .anthropic import (
    _collapse_stream,
    _extract_litellm_headers,
    _iter_with_prefix,
    _open_request,
)
from .translate import anthropic_sse_to_openai_sse, openai_to_anthropic

log = logging.getLogger(__name__)


def build_router(manager: SessionManager, auth_dep) -> APIRouter:
    router = APIRouter()

    @router.post("/v1/chat/completions")
    async def chat_completions(req: Request, pool: list[str] = Depends(auth_dep)) -> Any:
        oi_req = await req.json()
        model = oi_req.get("model", "claude-sonnet-4-5")
        wants_stream = bool(oi_req.get("stream", False))
        litellm = _extract_litellm_headers(req.headers)
        request_metadata = {"litellm": litellm} if litellm else None

        anth_req = openai_to_anthropic(oi_req)
        sess, channel, first_chunk = await _open_request(
            manager, pool, anth_req, request_metadata)

        if wants_stream:
            async def gen():
                async for sse in anthropic_sse_to_openai_sse(
                        _iter_with_prefix(channel, first_chunk), model):
                    yield sse
            return StreamingResponse(gen(), media_type="text/event-stream")

        message = await _collapse_stream(
            _iter_with_prefix(channel, first_chunk))
        return JSONResponse(_anthropic_message_to_openai(message, model))

    return router


def _anthropic_message_to_openai(message: dict[str, Any], model: str) -> dict[str, Any]:
    content_blocks = message.get("content", [])
    text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    for block in content_blocks:
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
        elif block.get("type") == "tool_use":
            tool_calls.append({
                "id": block.get("id"),
                "type": "function",
                "function": {
                    "name": block.get("name"),
                    "arguments": json.dumps(block.get("input", {})),
                },
            })
    msg: dict[str, Any] = {"role": "assistant", "content": "\n".join(text_parts) or None}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    finish = {
        "end_turn": "stop", "max_tokens": "length",
        "stop_sequence": "stop", "tool_use": "tool_calls",
    }.get(message.get("stop_reason"), "stop")
    usage = message.get("usage", {})
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": usage.get("input_tokens", 0),
            "completion_tokens": usage.get("output_tokens", 0),
            "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
        },
    }
