from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config
from .session import ClaudeSession

log = logging.getLogger(__name__)

# Wait up to this long for in-flight SSE streams to finish before tearing
# down a worker during a scheduled restart. After this we force the
# restart anyway; the truncated streams get a None sentinel via stop().
_RESTART_DRAIN_TIMEOUT = 60.0


class SessionManager:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.sessions: dict[str, ClaudeSession] = {}
        self.lock = asyncio.Lock()
        self._next_port_offset = 0
        self._restarter_task: asyncio.Task | None = None
        # Round-robin tiebreaker counter, keyed by the tuple of pool
        # members. Used only when every worker in the pool is busy.
        self._rr: dict[tuple[str, ...], int] = {}

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

    async def pick(self, pool: list[str]) -> ClaudeSession:
        """Pick a session from a token's user pool. With one member this
        is just get_or_create. With several, we prefer a worker that has
        no in-flight request (len(_channels) == 0); if every worker is
        busy, fall back to fewest-in-flight, breaking ties with a
        round-robin counter so a steady stream of requests gets spread
        across the pool rather than piling onto whichever worker happens
        to win the min() comparison repeatedly."""
        if len(pool) == 1:
            return await self.get_or_create(pool[0])
        sessions = [await self.get_or_create(u) for u in pool]
        idle = [s for s in sessions if not s._channels]
        if idle:
            # Among idle workers, round-robin too so a burst of fast
            # requests doesn't always hit pool[0].
            key = tuple(pool)
            idx = self._rr.get(key, 0) % len(idle)
            self._rr[key] = idx + 1
            return idle[idx]
        # All busy — fewest in-flight wins, RR breaks ties.
        min_inflight = min(len(s._channels) for s in sessions)
        candidates = [s for s in sessions if len(s._channels) == min_inflight]
        key = tuple(pool)
        idx = self._rr.get(key, 0) % len(candidates)
        self._rr[key] = idx + 1
        return candidates[idx]

    async def start(self) -> None:
        self._restarter_task = asyncio.create_task(self._restarter())
        # Prewarm: spawn a worker for every configured user during
        # container startup so the first request from each user does
        # not pay the cold-start cost (claude CLI TUI boot + mitm
        # bring-up, ~10s). Serial to avoid CPU/IO contention between
        # concurrently booting CLIs; per-user failures are logged but
        # do not block service startup, so a misconfigured user falls
        # back to lazy spawn on first request (same as before).
        user_ids: list[str] = []
        for pool in self.config.users.values():
            for u in pool:
                if u not in user_ids:
                    user_ids.append(u)
        for user_id in user_ids:
            try:
                await self.get_or_create(user_id)
                log.info("prewarmed user=%s", user_id)
            except Exception:
                log.exception("prewarm failed user=%s; "
                              "first request will cold-start", user_id)

    async def stop(self) -> None:
        if self._restarter_task:
            self._restarter_task.cancel()
        for sess in list(self.sessions.values()):
            await sess.stop()
        self.sessions.clear()

    async def _restarter(self) -> None:
        """Periodically recycle each worker in place so accumulated CLI
        state (Ink buffer, transcripts, cached tokens) gets cleared. The
        session object and its mitm port are reused; only the worker
        subprocess (and the claude/mitm processes it owns) is replaced."""
        interval = self.config.claude.restart_interval_seconds
        while True:
            try:
                await asyncio.sleep(60)
                for user_id, sess in list(self.sessions.items()):
                    if sess.age_seconds() <= interval:
                        continue
                    log.info("scheduled restart user=%s age=%.0fs",
                             user_id, sess.age_seconds())
                    async with sess.lock:
                        # Hold the session lock to block new submissions;
                        # wait for any already-streaming responses to
                        # finish before tearing the worker down.
                        deadline = time.monotonic() + _RESTART_DRAIN_TIMEOUT
                        while sess._channels and time.monotonic() < deadline:
                            await asyncio.sleep(0.5)
                        if sess._channels:
                            log.warning("restart user=%s force: %d streams still in flight",
                                        user_id, len(sess._channels))
                        try:
                            await sess.restart()
                            log.info("restart complete user=%s", user_id)
                        except Exception:
                            log.exception("restart failed user=%s; dropping session",
                                          user_id)
                            self.sessions.pop(user_id, None)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("restarter loop error")
