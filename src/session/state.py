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
    API handler reads until sentinel (None) is enqueued. Timestamps let
    /status distinguish a healthy in-flight request (bytes flowing) from
    a stalled / orphaned channel (no bytes ever, or no bytes for >30s)."""
    queue: asyncio.Queue[bytes | None] = field(default_factory=asyncio.Queue)
    created_at: float = field(default_factory=time.monotonic)
    last_chunk_at: float = field(default_factory=time.monotonic)

    async def put(self, chunk: bytes | None) -> None:
        await self.queue.put(chunk)

    async def iter(self):
        while True:
            chunk = await self.queue.get()
            if chunk is None:
                return
            yield chunk
