from __future__ import annotations

import json
import time
import uuid
from typing import Any, AsyncIterator


def openai_to_anthropic(req: dict[str, Any]) -> dict[str, Any]:
    """Translate an OpenAI Chat Completions request to Anthropic Messages.
    Handles: model, messages, system, max_tokens, temperature, top_p, stop,
    tools, tool_choice. Not exhaustive — extend as needed."""
    out: dict[str, Any] = {}
    out["model"] = req.get("model", "claude-sonnet-4-5")
    out["max_tokens"] = req.get("max_tokens", 4096)

    system_chunks: list[str] = []
    messages: list[dict[str, Any]] = []
    for m in req.get("messages", []):
        role = m.get("role")
        content = m.get("content")
        if role == "system":
            if isinstance(content, str):
                system_chunks.append(content)
            elif isinstance(content, list):
                for part in content:
                    if part.get("type") == "text":
                        system_chunks.append(part.get("text", ""))
            continue
        if role == "tool":
            messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": m.get("tool_call_id"),
                    "content": content if isinstance(content, str) else json.dumps(content),
                }],
            })
            continue
        if role == "assistant" and m.get("tool_calls"):
            blocks: list[dict[str, Any]] = []
            if isinstance(content, str) and content:
                blocks.append({"type": "text", "text": content})
            for tc in m["tool_calls"]:
                fn = tc.get("function", {})
                try:
                    args = json.loads(fn.get("arguments", "{}"))
                except json.JSONDecodeError:
                    args = {}
                blocks.append({
                    "type": "tool_use",
                    "id": tc.get("id"),
                    "name": fn.get("name"),
                    "input": args,
                })
            messages.append({"role": "assistant", "content": blocks})
            continue
        # plain user/assistant text or multimodal
        messages.append({"role": role, "content": content})

    if system_chunks:
        out["system"] = "\n\n".join(system_chunks)
    out["messages"] = messages

    for opt_key in ("temperature", "top_p", "stop_sequences"):
        if opt_key in req:
            out[opt_key] = req[opt_key]
    if "stop" in req and "stop_sequences" not in out:
        stop = req["stop"]
        out["stop_sequences"] = [stop] if isinstance(stop, str) else stop

    if "tools" in req:
        out["tools"] = [
            {
                "name": t["function"]["name"],
                "description": t["function"].get("description", ""),
                "input_schema": t["function"].get("parameters", {"type": "object"}),
            }
            for t in req["tools"] if t.get("type") == "function"
        ]
    if "tool_choice" in req:
        tc = req["tool_choice"]
        if tc == "auto":
            out["tool_choice"] = {"type": "auto"}
        elif tc == "none":
            pass  # Anthropic has no direct equivalent; omit tools or use {"type":"any"}
        elif isinstance(tc, dict) and tc.get("type") == "function":
            out["tool_choice"] = {"type": "tool", "name": tc["function"]["name"]}

    out["stream"] = bool(req.get("stream", False))
    return out


async def anthropic_sse_to_openai_sse(
    channel_iter: AsyncIterator[bytes], model: str,
) -> AsyncIterator[bytes]:
    """Convert Anthropic SSE byte stream to OpenAI Chat Completions SSE."""
    completion_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    created = int(time.time())
    buf = b""
    role_emitted = False
    content_blocks: dict[int, dict[str, Any]] = {}
    tool_index_to_oi_index: dict[int, int] = {}
    next_oi_tool_index = 0

    async for chunk in channel_iter:
        buf += chunk
        while b"\n\n" in buf:
            raw_event, buf = buf.split(b"\n\n", 1)
            event = _parse_sse_event(raw_event)
            if not event:
                continue
            etype = event["event"]
            data = event["data"]

            if etype == "message_start" and not role_emitted:
                role_emitted = True
                yield _oi_sse({
                    "id": completion_id, "object": "chat.completion.chunk",
                    "created": created, "model": model,
                    "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
                })
            elif etype == "content_block_start":
                idx = data.get("index", 0)
                block = data.get("content_block", {})
                content_blocks[idx] = block
                if block.get("type") == "tool_use":
                    oi_idx = next_oi_tool_index
                    next_oi_tool_index += 1
                    tool_index_to_oi_index[idx] = oi_idx
                    yield _oi_sse({
                        "id": completion_id, "object": "chat.completion.chunk",
                        "created": created, "model": model,
                        "choices": [{"index": 0, "delta": {"tool_calls": [{
                            "index": oi_idx,
                            "id": block.get("id"),
                            "type": "function",
                            "function": {"name": block.get("name"), "arguments": ""},
                        }]}, "finish_reason": None}],
                    })
            elif etype == "content_block_delta":
                idx = data.get("index", 0)
                delta = data.get("delta", {})
                dtype = delta.get("type")
                if dtype == "text_delta":
                    yield _oi_sse({
                        "id": completion_id, "object": "chat.completion.chunk",
                        "created": created, "model": model,
                        "choices": [{"index": 0, "delta": {"content": delta.get("text", "")}, "finish_reason": None}],
                    })
                elif dtype == "input_json_delta":
                    oi_idx = tool_index_to_oi_index.get(idx, 0)
                    yield _oi_sse({
                        "id": completion_id, "object": "chat.completion.chunk",
                        "created": created, "model": model,
                        "choices": [{"index": 0, "delta": {"tool_calls": [{
                            "index": oi_idx,
                            "function": {"arguments": delta.get("partial_json", "")},
                        }]}, "finish_reason": None}],
                    })
            elif etype == "message_delta":
                stop_reason = data.get("delta", {}).get("stop_reason")
                if stop_reason:
                    finish = {
                        "end_turn": "stop", "max_tokens": "length",
                        "stop_sequence": "stop", "tool_use": "tool_calls",
                    }.get(stop_reason, "stop")
                    yield _oi_sse({
                        "id": completion_id, "object": "chat.completion.chunk",
                        "created": created, "model": model,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
                    })
            elif etype == "message_stop":
                yield b"data: [DONE]\n\n"


def _oi_sse(payload: dict[str, Any]) -> bytes:
    return b"data: " + json.dumps(payload).encode() + b"\n\n"


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
