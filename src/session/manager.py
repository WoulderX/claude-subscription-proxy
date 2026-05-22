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
        # Two-phase: under self.lock we decide what to do and, if a
        # worker is born or reborn, acquire sess.lock so no concurrent
        # caller can grab it. We then release self.lock and run prewarm
        # while still holding sess.lock — this keeps the prewarm window
        # short (other users unaffected) while preventing a real user
        # request from racing in and firing the first /v1/messages on a
        # worker whose claude CLI hasn't done its lazy bootstrap yet
        # (which causes the 7-call burst -> per-OAuth rate limit).
        needs_prewarm = False
        async with self.lock:
            sess = self.sessions.get(user_id)
            if sess is not None:
                # Liveness check: a worker can die between requests
                # (claude CLI crash, OOM kill, mitm fault). Without this
                # the stale ClaudeSession lives on in the dict and the
                # next call() raises "worker not running" forever, since
                # nothing else evicts it before _restarter's age-based
                # cycle (default 12h).
                if sess.proc is None or sess.proc.returncode is not None:
                    rc = sess.proc.returncode if sess.proc else "never started"
                    log.warning("user=%s worker dead (rc=%s); reviving in place",
                                user_id, rc)
                    # Acquire sess.lock and hold it across both restart
                    # and the subsequent prewarm. Manual acquire (not
                    # `async with`) because the prewarm runs *after* we
                    # release self.lock — keeping it in a block here
                    # would either pin self.lock for the prewarm
                    # duration (blocks other users) or release sess.lock
                    # too early (lets a real request race ahead of
                    # prewarm).
                    await sess.lock.acquire()
                    try:
                        await sess.restart()
                        needs_prewarm = True
                    except Exception:
                        sess.lock.release()
                        log.exception("user=%s in-place revive failed; "
                                      "dropping session, cold-creating",
                                      user_id)
                        self.sessions.pop(user_id, None)
                        sess = None
            if sess is None:
                port = self.config.mitm.port_base + self._next_port_offset
                self._next_port_offset += 1
                sess = ClaudeSession(user_id=user_id, mitm_port=port,
                                     config=self.config)
                await sess.start()
                self.sessions[user_id] = sess
                await sess.lock.acquire()
                needs_prewarm = True

        if needs_prewarm:
            try:
                await self._safe_prewarm(sess)
            finally:
                sess.lock.release()
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
        # bring-up + lazy bootstrap, ~10s + 7 HTTP calls). Serial to
        # avoid CPU/IO contention between concurrently booting CLIs.
        # get_or_create now runs the bootstrap prewarm itself for any
        # freshly-born worker, so this loop is just "spawn each user".
        # Per-user failures are logged but do not block service startup;
        # the misconfigured user falls back to lazy spawn on first
        # request (which also includes its own prewarm).
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

    async def _safe_prewarm(self, sess: ClaudeSession) -> None:
        """Run bootstrap prewarm with timeout + error containment.
        Failure is non-fatal — the worker is still serviceable; the
        user may just see a rate_limit_error on their first request
        and need to retry. Caller must hold sess.lock so the dummy
        /v1/messages we submit can't be interleaved with a real one."""
        try:
            await asyncio.wait_for(self._prewarm_bootstrap(sess), timeout=60)
        except asyncio.TimeoutError:
            log.warning("bootstrap prewarm timed out user=%s; "
                        "first real request may hit rate limit", sess.user_id)
        except Exception:
            log.exception("bootstrap prewarm failed user=%s; "
                          "first real request may hit rate limit", sess.user_id)

    async def _prewarm_bootstrap(self, sess: ClaudeSession) -> None:
        """Force claude CLI's lazy bootstrap to run NOW by submitting
        one tiny dummy /v1/messages to a freshly-born worker. Caller
        must hold sess.lock; we use sess._submit (lock-free path) so
        the prewarm doesn't release the lock between restart and the
        dummy request — a real user request slipping in there would
        be the very thing the prewarm is supposed to protect against.

        Why this matters: a fresh claude CLI process fires 6 sibling
        HTTP calls (eval / grove / penguin_mode / claude_cli/bootstrap
        / mcp-registry pagination / mcp_servers) alongside its first
        /v1/messages, in ~30 ms — the per-OAuth rate limiter trips on
        that burst and the /v1/messages comes back rate_limit_error.
        By running this dummy call at startup / restart / revive, the
        burst happens while no user is waiting and the on-disk caches
        (shared via the .claude/ directory symlink) get populated, so
        subsequent worker spawns may also skip the burst.

        Model is haiku + max_tokens=1 so the call itself is ~free
        against the subscription quota."""
        body = {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "ok"}],
        }
        log.info("bootstrap prewarm starting user=%s", sess.user_id)
        channel = await sess._submit(body)
        # Drain and discard. We don't care about the content — the
        # value was in the side-effect HTTP calls that fired in
        # parallel with the /v1/messages request.
        async for _ in channel.iter():
            pass
        log.info("bootstrap prewarm complete user=%s", sess.user_id)

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
                            # Same rationale as get_or_create: a fresh
                            # claude CLI process needs its lazy
                            # bootstrap forced now, while we still hold
                            # sess.lock, or the first real user request
                            # post-restart will trip the per-OAuth rate
                            # limiter.
                            await self._safe_prewarm(sess)
                            log.info("restart complete user=%s", user_id)
                        except Exception:
                            log.exception("restart failed user=%s; dropping session",
                                          user_id)
                            self.sessions.pop(user_id, None)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("restarter loop error")
