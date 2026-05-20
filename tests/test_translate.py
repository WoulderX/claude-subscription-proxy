"""Unit tests for the OpenAI ↔ Anthropic translation layer."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.api.translate import anthropic_sse_to_openai_sse, openai_to_anthropic


def test_basic_chat_translation():
    oi = {
        "model": "gpt-4",
        "messages": [
            {"role": "system", "content": "you are helpful"},
            {"role": "user", "content": "hi"},
        ],
        "max_tokens": 100,
        "temperature": 0.5,
        "stream": True,
    }
    anth = openai_to_anthropic(oi)
    assert anth["model"] == "gpt-4"
    assert anth["system"] == "you are helpful"
    assert anth["messages"] == [{"role": "user", "content": "hi"}]
    assert anth["max_tokens"] == 100
    assert anth["temperature"] == 0.5
    assert anth["stream"] is True


def test_tool_calls_translation():
    oi = {
        "model": "gpt-4",
        "messages": [
            {"role": "user", "content": "what's the weather?"},
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "get_weather", "arguments": '{"city": "SF"}'},
            }]},
            {"role": "tool", "tool_call_id": "call_1", "content": "72F"},
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "lookup weather",
                "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
            },
        }],
        "tool_choice": "auto",
    }
    anth = openai_to_anthropic(oi)
    # tool definition translated
    assert anth["tools"][0]["name"] == "get_weather"
    assert anth["tools"][0]["input_schema"]["properties"]["city"]["type"] == "string"
    # tool_choice translated
    assert anth["tool_choice"] == {"type": "auto"}
    # assistant tool_call → tool_use block
    assistant = anth["messages"][1]
    assert assistant["role"] == "assistant"
    assert any(b["type"] == "tool_use" and b["name"] == "get_weather" for b in assistant["content"])
    # tool message → user/tool_result
    tool_msg = anth["messages"][2]
    assert tool_msg["role"] == "user"
    assert tool_msg["content"][0]["type"] == "tool_result"
    assert tool_msg["content"][0]["tool_use_id"] == "call_1"


def test_stop_translation():
    oi = {"model": "gpt-4", "messages": [], "stop": "END"}
    anth = openai_to_anthropic(oi)
    assert anth["stop_sequences"] == ["END"]


async def _collect_sse(events_bytes_list, model):
    async def gen():
        for b in events_bytes_list:
            yield b
    out = []
    async for chunk in anthropic_sse_to_openai_sse(gen(), model):
        out.append(chunk)
    return b"".join(out)


def test_sse_text_stream_translation():
    anth_events = [
        b"event: message_start\ndata: " + json.dumps({"message": {"id": "msg_1"}}).encode() + b"\n\n",
        b"event: content_block_start\ndata: " + json.dumps({"index": 0, "content_block": {"type": "text", "text": ""}}).encode() + b"\n\n",
        b"event: content_block_delta\ndata: " + json.dumps({"index": 0, "delta": {"type": "text_delta", "text": "Hello"}}).encode() + b"\n\n",
        b"event: content_block_delta\ndata: " + json.dumps({"index": 0, "delta": {"type": "text_delta", "text": " world"}}).encode() + b"\n\n",
        b"event: content_block_stop\ndata: " + json.dumps({"index": 0}).encode() + b"\n\n",
        b"event: message_delta\ndata: " + json.dumps({"delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 5}}).encode() + b"\n\n",
        b"event: message_stop\ndata: {}\n\n",
    ]
    out = asyncio.run(_collect_sse(anth_events, "claude-sonnet-4-6"))
    s = out.decode()
    assert '"role": "assistant"' in s
    assert '"content": "Hello"' in s
    assert '"content": " world"' in s
    assert '"finish_reason": "stop"' in s
    assert s.endswith("data: [DONE]\n\n")


def test_sse_tool_call_translation():
    anth_events = [
        b"event: message_start\ndata: " + json.dumps({"message": {"id": "msg_2"}}).encode() + b"\n\n",
        b"event: content_block_start\ndata: " + json.dumps({"index": 0, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "search"}}).encode() + b"\n\n",
        b"event: content_block_delta\ndata: " + json.dumps({"index": 0, "delta": {"type": "input_json_delta", "partial_json": '{"q":'}}).encode() + b"\n\n",
        b"event: content_block_delta\ndata: " + json.dumps({"index": 0, "delta": {"type": "input_json_delta", "partial_json": '"hi"}'}}).encode() + b"\n\n",
        b"event: message_delta\ndata: " + json.dumps({"delta": {"stop_reason": "tool_use"}}).encode() + b"\n\n",
        b"event: message_stop\ndata: {}\n\n",
    ]
    out = asyncio.run(_collect_sse(anth_events, "claude-sonnet-4-6"))
    s = out.decode()
    assert '"name": "search"' in s
    assert '"id": "toolu_1"' in s
    assert '"arguments": "{\\"q\\":"' in s
    assert '"finish_reason": "tool_calls"' in s


if __name__ == "__main__":
    test_basic_chat_translation()
    test_tool_calls_translation()
    test_stop_translation()
    test_sse_text_stream_translation()
    test_sse_tool_call_translation()
    print("all translate tests pass")
