from __future__ import annotations

import asyncio
import logging

from ..config import Config
from .session import ClaudeSession

log = logging.getLogger(__name__)


class SessionManager:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.sessions: dict[str, ClaudeSession] = {}
        self.lock = asyncio.Lock()
        self._next_port_offset = 0
        self._reaper_task: asyncio.Task | None = None

    async def get_or_create(self, user_id: str) -> ClaudeSession:
        async with self.lock:
            sess = self.sessions.get(user_id)
            if sess is not None:
                return sess
            port = self.config.mitm.port_base + self._next_port_offset
            self._next_port_offset += 1
            sess = ClaudeSession(user_id=user_id, mitm_port=port, config=self.config)
            await sess.start()
            self.sessions[user_id] = sess
            return sess

    async def start(self) -> None:
        self._reaper_task = asyncio.create_task(self._reaper())

    async def stop(self) -> None:
        if self._reaper_task:
            self._reaper_task.cancel()
        for sess in list(self.sessions.values()):
            await sess.stop()
        self.sessions.clear()

    async def _reaper(self) -> None:
        timeout = self.config.claude.idle_timeout_seconds
        while True:
            try:
                await asyncio.sleep(60)
                for user_id, sess in list(self.sessions.items()):
                    if sess.idle_seconds() > timeout:
                        log.info("reaping idle session user=%s", user_id)
                        await sess.stop()
                        self.sessions.pop(user_id, None)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("reaper loop error")
