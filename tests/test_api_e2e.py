"""End-to-end API test that bypasses real mitm/PTY/claude.

We monkey-patch SessionManager.get_or_create to hand back a fake session
whose .call() synthesizes a canned Anthropic SSE stream. This validates
the entire FastAPI → router → manager → channel → SSE forwarding path
(including non-streaming collapse and OpenAI translation) without
needing real network or a real claude binary."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

from src.config import Config
from src.main import create_app
from src.session.state import ResponseChannel


CANNED_SSE = [
    b"event: message_start\ndata: " + json.dumps({"message": {
        "id": "msg_test", "type": "message", "role": "assistant",
        "model": "claude-sonnet-4-6", "content": [], "stop_reason": None,
        "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 0},
    }}).encode() + b"\n\n",
    b"event: content_block_start\ndata: " + json.dumps({
        "index": 0, "content_block": {"type": "text", "text": ""}}).encode() + b"\n\n",
    b"event: content_block_delta\ndata: " + json.dumps({
        "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}).encode() + b"\n\n",
    b"event: content_block_delta\ndata: " + json.dumps({
        "index": 0, "delta": {"type": "text_delta", "text": " from canned"}}).encode() + b"\n\n",
    b"event: content_block_stop\ndata: " + json.dumps({"index": 0}).encode() + b"\n\n",
    b"event: message_delta\ndata: " + json.dumps({
        "delta": {"stop_reason": "end_turn", "stop_sequence": None},
        "usage": {"output_tokens": 4}}).encode() + b"\n\n",
    b"event: message_stop\ndata: {}\n\n",
]


class FakeSession:
    def __init__(self, chunks=None):
        self.last_body = None
        self._chunks = chunks if chunks is not None else CANNED_SSE

    async def call(self, body):
        self.last_body = body
        channel = ResponseChannel()
        for ev in self._chunks:
            channel.queue.put_nowait(ev)
        channel.queue.put_nowait(None)
        return channel


class FakeManager:
    def __init__(self, chunks=None):
        self.sessions = {}
        self._chunks = chunks

    async def get_or_create(self, user_id):
        if user_id not in self.sessions:
            self.sessions[user_id] = FakeSession(self._chunks)
        return self.sessions[user_id]

    async def start(self): pass
    async def stop(self): pass


async def _run_tests():
    cfg = Config.model_validate({
        "listen_host": "127.0.0.1", "listen_port": 0,
        "users": {"sk-test": "alice"},
    })

    # Patch SessionManager in main to be our fake.
    with patch("src.main.SessionManager", lambda config: FakeManager()):
        app = create_app(cfg)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            # --- 1. health ---
            r = await client.get("/healthz")
            assert r.status_code == 200, r.text

            # --- 2. /v1/messages non-stream ---
            r = await client.post("/v1/messages",
                headers={"Authorization": "Bearer sk-test"},
                json={"model": "claude-sonnet-4-6",
                      "max_tokens": 50,
                      "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["content"][0]["text"] == "Hello from canned"
            assert body["stop_reason"] == "end_turn"
            print("  ok: /v1/messages non-stream")

            # --- 3. /v1/messages streaming ---
            chunks = []
            async with client.stream("POST", "/v1/messages",
                headers={"Authorization": "Bearer sk-test"},
                json={"model": "claude-sonnet-4-6",
                      "max_tokens": 50, "stream": True,
                      "messages": [{"role": "user", "content": "hi"}]}) as r:
                assert r.status_code == 200
                async for c in r.aiter_bytes():
                    chunks.append(c)
            stream = b"".join(chunks).decode()
            assert "message_start" in stream
            assert "Hello" in stream
            assert "message_stop" in stream
            print("  ok: /v1/messages stream")

            # --- 4. OpenAI /v1/chat/completions non-stream ---
            r = await client.post("/v1/chat/completions",
                headers={"Authorization": "Bearer sk-test"},
                json={"model": "gpt-4",
                      "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["object"] == "chat.completion"
            assert body["choices"][0]["message"]["content"] == "Hello from canned"
            assert body["choices"][0]["finish_reason"] == "stop"
            print("  ok: /v1/chat/completions non-stream")

            # --- 5. OpenAI /v1/chat/completions stream ---
            chunks = []
            async with client.stream("POST", "/v1/chat/completions",
                headers={"Authorization": "Bearer sk-test"},
                json={"model": "gpt-4", "stream": True,
                      "messages": [{"role": "user", "content": "hi"}]}) as r:
                assert r.status_code == 200
                async for c in r.aiter_bytes():
                    chunks.append(c)
            stream = b"".join(chunks).decode()
            assert "chat.completion.chunk" in stream
            assert '"content": "Hello"' in stream
            assert "[DONE]" in stream
            print("  ok: /v1/chat/completions stream")

            # --- 6. auth rejection ---
            r = await client.post("/v1/messages",
                json={"model": "x", "messages": []})
            assert r.status_code == 401
            r = await client.post("/v1/messages",
                headers={"Authorization": "Bearer wrong"},
                json={"model": "x", "messages": []})
            assert r.status_code == 401
            print("  ok: auth rejection")

            # --- 7. x-api-key works too ---
            r = await client.post("/v1/messages",
                headers={"x-api-key": "sk-test"},
                json={"model": "claude-sonnet-4-6",
                      "max_tokens": 50,
                      "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200
            print("  ok: x-api-key auth")

    # --- 8. non-SSE upstream error body is surfaced (regression) ---
    # When Anthropic returns a 4xx with a plain JSON error (no SSE
    # framing), _collapse_stream used to silently drop it and return
    # empty content with stop_reason=end_turn. It should now surface
    # the error as stop_reason=error with [upstream ...] text.
    err_body = (b'{"type":"error","error":{"type":"authentication_error",'
                b'"message":"Invalid authentication credentials"},'
                b'"request_id":"req_xyz"}')
    with patch("src.main.SessionManager",
               lambda config: FakeManager(chunks=[err_body])):
        app = create_app(cfg)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            r = await client.post("/v1/messages",
                headers={"Authorization": "Bearer sk-test"},
                json={"model": "claude-sonnet-4-6",
                      "max_tokens": 50,
                      "messages": [{"role": "user", "content": "hi"}]})
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["stop_reason"] == "error", body
            assert body["content"], "content should not be empty on error"
            text = body["content"][0]["text"]
            assert "authentication_error" in text, text
            assert "Invalid authentication credentials" in text, text
            print("  ok: non-SSE upstream error surfaced as stop_reason=error")

    print("all e2e tests pass")


if __name__ == "__main__":
    asyncio.run(_run_tests())
