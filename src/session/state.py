from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PendingRequest:
    """A user-originated request waiting for the next outbound /v1/messages
    from this session's claude code process to hijack."""
    # full Anthropic /v1/messages JSON body to substitute
    body: dict[str, Any]
    # event signaled by the mitm addon once it has actually swapped the request
    consumed: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class ResponseChannel:
    """Mitm addon writes raw SSE bytes here as they stream in from Anthropic.
    API handler reads until sentinel (None) is enqueued. Timestamps and
    body_summary let /status distinguish a healthy in-flight request
    (bytes flowing, recent chunk) from a stalled / orphaned channel
    (no bytes ever, or no bytes for >30s), and show what each worker
    is actually processing right now."""
    queue: asyncio.Queue[bytes | None] = field(default_factory=asyncio.Queue)
    created_at: float = field(default_factory=time.monotonic)
    last_chunk_at: float = field(default_factory=time.monotonic)
    # Total response bytes that have flowed through this channel from
    # mitm to the API handler so far. Bumped by session._read_loop.
    bytes_received: int = 0
    # Small log-safe summary of the request that opened this channel
    # (model / max_tokens / n_messages / preview of last user message).
    # Populated by ClaudeSession.call at submission time so operators
    # can see what a stuck worker was trying to do without expanding
    # the full request body.
    body_summary: dict[str, Any] = field(default_factory=dict)
    # Rate-limit scan state. Anthropic emits `rate_limit_error` in the
    # very first SSE event (or as the HTTP 429 body), so a 4 KB head
    # buffer is more than enough to spot it deterministically — we
    # don't need to look at the rest of the stream. Once we either
    # detect the marker or pass the head threshold, scanning stops and
    # the buffer is freed.
    _rl_head: bytearray = field(default_factory=bytearray)
    _rl_scanned: bool = False
    # Token usage extracted from the response stream by the mitm addon
    # (parsed off message_start + message_delta SSE events). Populated on
    # end-of-stream; consumed by worker._handle to emit a "usage" IPC
    # message tagged with this request's id. None = no usage observed
    # (typical for upstream errors / aborted streams).
    usage: dict[str, Any] | None = None

    async def put(self, chunk: bytes | None) -> None:
        await self.queue.put(chunk)

    async def iter(self):
        while True:
            chunk = await self.queue.get()
            if chunk is None:
                return
            yield chunk
