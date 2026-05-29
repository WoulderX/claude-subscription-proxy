"""Unit tests for _SSEFilter — strips tool_use blocks from the SSE
stream that the worker-side claude TUI sees, while leaving the
upstream bytes that go to the API caller untouched."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.mitm.addon import _SSEFilter


def _event(name: str, data: dict) -> bytes:
    return f"event: {name}\ndata: {json.dumps(data)}\n\n".encode("utf-8")


def _parse_events(blob: bytes) -> list[dict]:
    """Parse a stream of SSE events back into [{"event": str, "data": dict}]."""
    out = []
    for block in blob.split(b"\n\n"):
        block = block.strip()
        if not block:
            continue
        event_name = None
        data_parts = []
        for line in block.split(b"\n"):
            if line.startswith(b"event:"):
                event_name = line[6:].strip().decode()
            elif line.startswith(b"data:"):
                data_parts.append(line[5:].lstrip())
        out.append({
            "event": event_name,
            "data": json.loads(b"".join(data_parts)) if data_parts else None,
        })
    return out


def test_text_only_response_passes_through_unchanged():
    f = _SSEFilter()
    stream = (
        _event("message_start", {"type": "message_start", "message": {"id": "m1"}})
        + _event("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}})
        + _event("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 0})
        + _event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1}})
        + _event("message_stop", {"type": "message_stop"})
    )
    out = f.feed(stream) + f.feed(b"")
    assert out == stream


def test_tool_use_block_is_stripped():
    f = _SSEFilter()
    stream = (
        _event("message_start", {"type": "message_start", "message": {"id": "m1"}})
        + _event("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_x", "name": "Bash", "input": {}}})
        + _event("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"command\":\"ls\"}"}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 0})
        + _event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 5}})
        + _event("message_stop", {"type": "message_stop"})
    )
    out = f.feed(stream) + f.feed(b"")
    events = _parse_events(out)
    types = [e["data"]["type"] for e in events]
    # All three tool_use lifecycle events are gone
    assert "content_block_start" not in types
    assert "content_block_delta" not in types
    assert "content_block_stop" not in types
    # message_delta survived but with rewritten stop_reason
    md = next(e for e in events if e["data"]["type"] == "message_delta")
    assert md["data"]["delta"]["stop_reason"] == "end_turn"
    # message_start + message_delta + message_stop only
    assert types == ["message_start", "message_delta", "message_stop"]


def test_mixed_text_and_tool_use_keeps_text_drops_tool():
    f = _SSEFilter()
    stream = (
        _event("message_start", {"type": "message_start", "message": {"id": "m1"}})
        # text block at index 0
        + _event("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}})
        + _event("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": "Let me check."}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 0})
        # tool_use block at index 1
        + _event("content_block_start", {
            "type": "content_block_start", "index": 1,
            "content_block": {"type": "tool_use", "id": "toolu_y", "name": "Read", "input": {}}})
        + _event("content_block_delta", {
            "type": "content_block_delta", "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{}"}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 1})
        + _event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 8}})
        + _event("message_stop", {"type": "message_stop"})
    )
    out = f.feed(stream) + f.feed(b"")
    events = _parse_events(out)
    types = [e["data"]["type"] for e in events]
    # Text block fully present (start, one delta, stop)
    assert types.count("content_block_start") == 1
    assert types.count("content_block_delta") == 1
    assert types.count("content_block_stop") == 1
    # The surviving text block is at index 0
    text_start = next(e for e in events if e["data"]["type"] == "content_block_start")
    assert text_start["data"]["index"] == 0
    assert text_start["data"]["content_block"]["type"] == "text"
    # message_delta rewritten
    md = next(e for e in events if e["data"]["type"] == "message_delta")
    assert md["data"]["delta"]["stop_reason"] == "end_turn"


def test_byte_level_chunk_splits_are_buffered():
    """Anthropic SSE arrives in arbitrary chunk sizes — splits can land
    mid-event or even mid-line. The filter must buffer until a complete
    event (\\n\\n delimited) is in hand."""
    f = _SSEFilter()
    stream = (
        _event("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "tx", "name": "B", "input": {}}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 0})
    )
    # Feed one byte at a time
    out = bytearray()
    for i in range(len(stream)):
        out.extend(f.feed(stream[i:i + 1]))
    out.extend(f.feed(b""))
    # Whole stream was tool_use → nothing should reach the TUI
    assert bytes(out) == b""


def test_multiple_tool_use_blocks_tracked_independently():
    f = _SSEFilter()
    stream = (
        _event("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "a", "name": "B", "input": {}}})
        + _event("content_block_start", {
            "type": "content_block_start", "index": 1,
            "content_block": {"type": "tool_use", "id": "b", "name": "R", "input": {}}})
        + _event("content_block_delta", {
            "type": "content_block_delta", "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{}"}})
        + _event("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{}"}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 0})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 1})
    )
    out = f.feed(stream) + f.feed(b"")
    assert out == b""


def test_thinking_block_passes_through():
    f = _SSEFilter()
    stream = _event("content_block_start", {
        "type": "content_block_start", "index": 0,
        "content_block": {"type": "thinking", "thinking": ""}})
    out = f.feed(stream) + f.feed(b"")
    assert out == stream


def test_server_tool_use_also_stripped():
    f = _SSEFilter()
    stream = (
        _event("content_block_start", {
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "server_tool_use", "id": "s", "name": "WebSearch", "input": {}}})
        + _event("content_block_stop", {"type": "content_block_stop", "index": 0})
    )
    out = f.feed(stream) + f.feed(b"")
    assert out == b""


def test_heartbeat_comment_passes_through():
    f = _SSEFilter()
    # SSE comment / keep-alive (line starting with `:`)
    stream = b": keep-alive\n\n"
    out = f.feed(stream) + f.feed(b"")
    assert out == stream


def test_message_delta_with_non_tool_use_stop_reason_unchanged():
    f = _SSEFilter()
    stream = _event("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": "max_tokens"},
        "usage": {"output_tokens": 100}})
    out = f.feed(stream) + f.feed(b"")
    assert out == stream
