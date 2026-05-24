from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from ..config import Config
from ..usage import UsageStore
from .session import ClaudeSession

log = logging.getLogger(__name__)


@dataclass
class AccountIssue:
    """Account-scoped routing-block marker. While `until` is in the
    future, pick() avoids routing any request to a worker on this
    account (provided there's at least one usable alternative). When
    `until` passes, the next real request serves as a recheck probe —
    if still problematic, the SSE detector / IPC event will re-mark
    it; if cleared, the request succeeds and the marker stays expired.

    Two `kind` values, displayed differently on /ui:

      "rate_limit"  — we have positive evidence the account hit an
                      Anthropic quota. Source: SSE response containing
                      "rate_limit_error", or claude TUI modal saying
                      "resets MMM DD". `reason` is one of
                      "weekly_limit", "5hour_limit", "rate_limit",
                      "manual" (operator-set).

      "degraded"    — the account looks broken but we don't know why.
                      Most often: prewarm timed out without mitm ever
                      seeing /v1/messages (could be perms, expired
                      token, TUI hang, Anthropic flake). `reason` is
                      typically "prewarm-failed". Operator should
                      inspect logs.

    Distinction matters because the operator response is different:
    rate_limit will clear itself at the announced reset time, degraded
    likely needs human investigation. Both block routing the same way."""
    kind: str           # "rate_limit" | "degraded"
    reason: str         # human-readable subtype within `kind`
    set_at: float       # epoch seconds (wall-clock, for UI display)
    until: float        # epoch seconds


class SessionManager:
    def __init__(self, config: Config,
                 usage_store: UsageStore | None = None) -> None:
        self.config = config
        # Sqlite-backed token-usage log. None when usage.enabled=false;
        # _on_worker_usage short-circuits in that case so the worker
        # still emits the IPC (cheap) but we silently drop it.
        self.usage_store = usage_store
        self.sessions: dict[str, ClaudeSession] = {}
        self.lock = asyncio.Lock()
        self._next_port_offset = 0
        self._restarter_task: asyncio.Task | None = None
        # Round-robin tiebreaker counter, keyed by the tuple of pool
        # members. Used only when every worker in the pool is busy.
        self._rr: dict[tuple[str, ...], int] = {}
        # Per-account routing-block table. Keyed by account name from
        # the `accounts:` config block; legacy single-account deployments
        # never populate this. Entries have a `kind` field that the UI
        # uses to distinguish "real rate-limit" from "degraded /
        # unknown failure" — both block routing, but the operator's
        # next move differs (wait for reset vs. investigate).
        self._account_rl: dict[str, AccountIssue] = {}

    # ---------- account rate-limit table ----------

    def _account_name_for(self, user_id: str) -> str | None:
        """Reverse-lookup the account name from a worker user_id. None in
        legacy single-account mode (no `accounts:` configured)."""
        acc = self.config.account_for_user(user_id)
        if acc is None:
            return None
        for name, a in self.config.accounts.items():
            if a is acc:
                return name
        return None

    def mark_account_rate_limited(self, account_name: str, reason: str,
                                  window_seconds: float) -> None:
        """Record that an account hit a positive rate-limit signal
        (SSE error / TUI modal / 429). See `_mark_account_issue` for
        the merge semantics — short windows don't shrink a longer
        existing window, but a rate_limit observation upgrades a prior
        `degraded` mark (we now know it's quota, not perms)."""
        self._mark_account_issue(
            account_name, kind="rate_limit", reason=reason,
            window_seconds=window_seconds)

    def mark_account_degraded(self, account_name: str, reason: str,
                              window_seconds: float) -> None:
        """Record that an account's worker is unusable for an unknown
        reason (prewarm timeout, claude CLI hang, etc.). Like rate-limit
        it blocks routing — but the UI labels it differently so the
        operator knows to investigate vs. wait for a reset moment."""
        self._mark_account_issue(
            account_name, kind="degraded", reason=reason,
            window_seconds=window_seconds)

    def _mark_account_issue(self, account_name: str, *, kind: str,
                            reason: str, window_seconds: float) -> None:
        """Common path for both kinds. Merge rules:

          - Existing `until` in the past → replace.
          - Existing `rate_limit`, new `degraded` → keep existing
            (don't downgrade positive evidence with "unknown").
          - Same kind → keep the LONGER `until` (account is blocked
            until all known limits clear) but ADOPT the new `reason`.
            A re-detection has fresher info about which window applies
            — sticking with a stale reason produced a real-world bug
            where "5hour_limit" wording from a body hint persisted
            despite a later 4-day reset that obviously meant weekly.
          - Existing `degraded`, new `rate_limit` → upgrade outright
            (specific evidence beats unknown, even if new window is
            shorter).
        """
        now = time.time()
        new_until = now + window_seconds
        existing = self._account_rl.get(account_name)
        if existing is not None and existing.until > now:
            if existing.kind == "rate_limit" and kind == "degraded":
                # Don't downgrade. Just refresh set_at.
                existing.set_at = now
                return
            if existing.kind == kind:
                # Same kind: take longer until, latest reason. Latest
                # reason wins because the new detection has more
                # information (often a more accurate classifier based
                # on the actual delta).
                if reason != existing.reason:
                    log.info("account=%s reason refined: %s -> %s "
                             "(window now %s)",
                             account_name, existing.reason, reason,
                             time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                           time.gmtime(max(existing.until, new_until))))
                existing.reason = reason
                existing.until = max(existing.until, new_until)
                existing.set_at = now
                return
            # Different kind (degraded → rate_limit upgrade), fall
            # through to replace.
        self._account_rl[account_name] = AccountIssue(
            kind=kind, reason=reason, set_at=now, until=new_until)
        log.warning("account=%s %s (%s); routing will skip it for "
                    "the next %.0fs",
                    account_name, kind, reason, window_seconds)

    def is_account_rate_limited(self, account_name: str) -> bool:
        """Returns True iff the account is currently in its limit window.
        Expired entries are garbage-collected lazily on read."""
        state = self._account_rl.get(account_name)
        if state is None:
            return False
        if time.time() >= state.until:
            self._account_rl.pop(account_name, None)
            log.info("account=%s rate-limit window expired", account_name)
            return False
        return True

    def clear_account_rate_limit(self, account_name: str) -> bool:
        """Manually clear an account's limit (admin endpoint hook).
        Returns True if a marker was removed."""
        return self._account_rl.pop(account_name, None) is not None

    def mark_account_rate_limited_until(self, account_name: str,
                                        reason: str,
                                        until_epoch: float) -> None:
        """Variant of mark_account_rate_limited that takes an explicit
        absolute reset moment. Used by the manual-override admin
        endpoint — operator's input is authoritative, we record it
        verbatim regardless of any heuristic mark already present.
        Always recorded as `kind=rate_limit` (operator stated the
        reset time, so quota is the implicit cause)."""
        now = time.time()
        if until_epoch <= now:
            # Past timestamp = clear marker
            self._account_rl.pop(account_name, None)
            log.info("manual rate-limit set with past timestamp for "
                     "account=%s; cleared instead", account_name)
            return
        self._account_rl[account_name] = AccountIssue(
            kind="rate_limit", reason=reason,
            set_at=now, until=until_epoch)
        log.warning("account=%s rate-limited (%s, manual) until %s",
                    account_name, reason,
                    time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                  time.gmtime(until_epoch)))

    def account_rate_limit(self, account_name: str) -> AccountIssue | None:
        """Snapshot accessor for /status. Garbage-collects expired
        entries on read so callers always see a fresh view. Name kept
        for back-compat — covers both rate_limit and degraded kinds."""
        state = self._account_rl.get(account_name)
        if state is None:
            return None
        if time.time() >= state.until:
            self._account_rl.pop(account_name, None)
            return None
        return state

    def _on_worker_usage(self, user_id: str, payload: dict) -> None:
        """Callback wired into every ClaudeSession; receives token-usage
        events parsed off Anthropic SSE responses. Resolves the worker
        to its account name and writes one row to UsageStore.

        Best-effort: any exception is logged but never propagates — the
        usage log is observability, the request was already served."""
        if self.usage_store is None:
            return
        try:
            account = self._account_name_for(user_id)
            self.usage_store.record(
                ts=time.time(),
                account=account,
                worker=user_id,
                litellm_user=payload.get("litellm_user"),
                model=payload.get("model"),
                input_tokens=int(payload.get("input_tokens") or 0),
                output_tokens=int(payload.get("output_tokens") or 0),
                cache_creation_tokens=int(
                    payload.get("cache_creation_tokens") or 0),
                cache_read_tokens=int(
                    payload.get("cache_read_tokens") or 0),
            )
        except Exception:
            log.exception("usage record failed user=%s payload=%s",
                          user_id, payload)

    def lifecycle_since(self, group_by: str, key: str) -> float:
        """Resolve the `range=lifecycle` query's lower bound for a given
        group bucket. Lifecycle is per-worker conceptually; for grouped
        views we pick the EARLIEST started_at_wall among workers that
        currently match the bucket so the user sees a continuous span
        for an account that has multiple workers restarted at different
        times.

        Returns 0.0 when there's no matching live worker — the caller
        will get an empty result set, which the UI renders as "暂无
        数据 for this scope"."""
        oldest: float | None = None
        for user_id, sess in self.sessions.items():
            wall = sess.started_at_wall
            include = False
            if group_by == "worker":
                include = (user_id == key)
            elif group_by == "account":
                include = (self._account_name_for(user_id) == key)
            elif group_by == "litellm_user":
                # We don't track per-litellm-user worker affinity (any
                # litellm user can land on any worker in their pool), so
                # the best we can do is "earliest live worker overall".
                # The caller's question is "how much has this user
                # consumed since the proxy could route to a fresh
                # worker?" — earliest-overall answers that.
                include = True
            if include and (oldest is None or wall < oldest):
                oldest = wall
        return oldest or 0.0

    def _on_worker_rate_limited(self, user_id: str, reason: str,
                                window_seconds: float) -> None:
        """Callback wired into every ClaudeSession; fires when its
        SSE stream's head contained `rate_limit_error`. Resolves the
        worker → account and marks the account."""
        acc_name = self._account_name_for(user_id)
        if acc_name is None:
            # Legacy single-account mode: no account scope to mark. Log
            # so the operator at least sees the event in the proxy log.
            log.warning("user=%s hit rate_limit_error (%s) — no account "
                        "scope to mark in legacy mode", user_id, reason)
            return
        self.mark_account_rate_limited(acc_name, reason, window_seconds)

    # ---------- session lifecycle ----------

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
                                     config=self.config,
                                     on_rate_limit=self._on_worker_rate_limited,
                                     on_usage=self._on_worker_usage)
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
        """Pick a session from a token's user pool.

        Selection layers (most-preferred first):
          1. Workers whose account is NOT rate-limited.
             If all alternatives are rate-limited we fall back to the
             whole pool — at least one of them needs to handle the
             request, and going through a limited account at least lets
             Anthropic re-confirm whether the limit still applies.
          2. Workers with no in-flight request (idle).
          3. Fewest-in-flight, RR-tied so the load spreads instead of
             piling on whichever worker won the min() comparison first.
        """
        if len(pool) == 1:
            return await self.get_or_create(pool[0])
        sessions = [await self.get_or_create(u) for u in pool]

        # Filter out rate-limited accounts when there's something left.
        usable = [
            s for s in sessions
            if not self._is_session_account_rate_limited(s)
        ]
        if not usable:
            log.warning("all %d workers in pool are on rate-limited "
                        "accounts; routing through anyway", len(sessions))
            usable = sessions

        idle = [s for s in usable if not s._channels]
        key = tuple(pool)
        if idle:
            # Among idle workers, round-robin too so a burst of fast
            # requests doesn't always hit pool[0].
            idx = self._rr.get(key, 0) % len(idle)
            self._rr[key] = idx + 1
            return idle[idx]
        # All busy — fewest in-flight wins, RR breaks ties.
        min_inflight = min(len(s._channels) for s in usable)
        candidates = [s for s in usable if len(s._channels) == min_inflight]
        idx = self._rr.get(key, 0) % len(candidates)
        self._rr[key] = idx + 1
        return candidates[idx]

    def _is_session_account_rate_limited(self, sess: ClaudeSession) -> bool:
        acc_name = self._account_name_for(sess.user_id)
        if acc_name is None:
            return False
        return self.is_account_rate_limited(acc_name)

    async def start(self) -> None:
        self._restarter_task = asyncio.create_task(self._restarter())
        # Prewarm: spawn a worker for every configured user during
        # container startup so the first request from each user does
        # not pay the cold-start cost (claude CLI TUI boot + mitm
        # bring-up + lazy bootstrap, ~10s + 6 sibling HTTP calls).
        #
        # Layout:
        #   - Across accounts: PARALLEL. Each account has its own OAuth
        #     identity, so the per-OAuth rate limiter that the bootstrap
        #     burst trips is account-scoped — parallelizing across
        #     accounts gets a clean linear speedup with no extra burst
        #     risk.
        #   - Within an account: SERIAL. 5 workers on the same OAuth
        #     would mean 5×6=30 bootstrap calls in <100ms — well past
        #     the per-OAuth rate limit. The first worker's prewarm fills
        #     the .claude/ on-disk caches (mcp-registry etc.) so workers
        #     2..N MAY hit a smaller burst (some calls are per-process
        #     and cannot be cached); even so, serial keeps them out of
        #     each other's way.
        #
        # Per-user failures are logged but do not block service startup
        # or other accounts' chains; the misconfigured user falls back
        # to lazy spawn on first request (which includes its own prewarm).
        user_ids: list[str] = []
        for pool in self.config.users.values():
            for u in pool:
                if u not in user_ids:
                    user_ids.append(u)

        # Group user_ids by account. user_ids whose account_for_user
        # returns None (legacy single-account mode) all land in the
        # same group, so behavior matches the old all-serial loop.
        by_account: dict[str, list[str]] = {}
        for u in user_ids:
            acc = self.config.account_for_user(u)
            key = acc.dir if acc is not None else "_legacy"
            by_account.setdefault(key, []).append(u)

        # Allocate ports + register placeholder sessions under self.lock
        # in one quick pass. Done before kicking off the parallel chains
        # because self.lock is held by get_or_create during sess.start()
        # — if we let _chain() call get_or_create() naively, the per-
        # account chains would all serialise on that lock and we'd lose
        # the parallelism we just engineered. Doing port allocation up
        # front and then spawning + prewarming outside the lock keeps
        # the lock window to a few microseconds per session.
        chains: dict[str, list[ClaudeSession]] = {}
        async with self.lock:
            for key, uids in by_account.items():
                chain: list[ClaudeSession] = []
                for uid in uids:
                    if uid in self.sessions:
                        # Already prewarmed (extremely unlikely on cold
                        # start, but handle defensively).
                        continue
                    port = self.config.mitm.port_base + self._next_port_offset
                    self._next_port_offset += 1
                    sess = ClaudeSession(user_id=uid, mitm_port=port,
                                         config=self.config,
                                         on_rate_limit=self._on_worker_rate_limited,
                                         on_usage=self._on_worker_usage)
                    self.sessions[uid] = sess
                    # Hold sess.lock immediately so a real request that
                    # somehow races in (shouldn't on startup, but defensive)
                    # waits behind start+prewarm.
                    await sess.lock.acquire()
                    chain.append(sess)
                chains[key] = chain

        async def _spawn_only(sess: ClaudeSession) -> None:
            """Spawn the PTY + mitm listener without firing the dummy
            haiku. Used for workers 2..N when the first worker on the
            same account already failed prewarm — those workers will
            cold-start on demand when the account becomes usable again
            (typical case: weekly rate limit window resets)."""
            try:
                await sess.start()
                log.info("spawned user=%s (prewarm skipped: "
                         "account degraded)", sess.user_id)
            except Exception:
                log.exception("spawn failed user=%s; dropping session",
                              sess.user_id)
                self.sessions.pop(sess.user_id, None)
            finally:
                if sess.lock.locked():
                    sess.lock.release()

        async def _spawn_and_prewarm(sess: ClaudeSession) -> bool:
            """Full path: spawn + dummy haiku. Returns prewarm success.
            False here triggers the fail-fast path for the rest of the
            account chain — we don't want to pay N × 60s of prewarm
            timeout for an account that's currently unusable."""
            try:
                await sess.start()
            except Exception:
                log.exception("spawn failed user=%s; dropping session",
                              sess.user_id)
                self.sessions.pop(sess.user_id, None)
                if sess.lock.locked():
                    sess.lock.release()
                return False
            try:
                ok = await self._safe_prewarm(sess)
                if ok:
                    log.info("prewarmed user=%s", sess.user_id)
                return ok
            finally:
                if sess.lock.locked():
                    sess.lock.release()

        async def _chain(chain_sessions: list[ClaudeSession]) -> None:
            if not chain_sessions:
                return
            first, *rest = chain_sessions
            first_ok = await _spawn_and_prewarm(first)
            if first_ok or not rest:
                # Normal path: continue serial spawn + prewarm. Serial
                # within an account is what keeps the per-OAuth bootstrap
                # burst (6 sibling HTTP calls per fresh CLI) from
                # tripping Anthropic's rate limiter.
                for sess in rest:
                    await _spawn_and_prewarm(sess)
            else:
                # Fail-fast path: first worker's prewarm failed (mitm
                # never intercepted → most often weekly rate-limit modal,
                # auth issue, or upstream sad-state). Repeating the same
                # prewarm for workers 2..N would burn 60s × (N-1) for
                # the SAME predictable failure. Instead spawn the rest
                # in PARALLEL (no per-OAuth burst worry since the dummy
                # /v1/messages isn't firing) so they're ready to serve
                # the moment the account becomes usable again.
                #
                # Conservatively mark the account as DEGRADED (not
                # rate-limited) for 5 min. The distinction matters in
                # the UI: prewarm failures can come from perms,
                # expired tokens, claude-CLI hangs, or actual quota —
                # we don't know which from the timeout alone. Routing
                # skips the account either way. If the worker side
                # later succeeds in parsing a precise reset time from
                # the TUI / SSE, an upgrade to kind=rate_limit will
                # supersede this degraded mark.
                acc_name = self._account_name_for(first.user_id)
                if acc_name is not None:
                    self.mark_account_degraded(
                        acc_name, "prewarm-failed", 300.0)
                log.warning("account chain fail-fast: %d more workers "
                            "will spawn without prewarm after %s's prewarm "
                            "failed", len(rest), first.user_id)
                await asyncio.gather(*[_spawn_only(s) for s in rest])

        await asyncio.gather(*[_chain(c) for c in chains.values()])

    async def _safe_prewarm(self, sess: ClaudeSession) -> bool:
        """Run bootstrap prewarm with timeout + error containment.
        Returns True on a clean run, False on any timeout/exception.

        Failure is non-fatal — the worker is still serviceable; the
        user may just see a rate_limit_error on their first request
        and need to retry. The bool lets the startup chain fail-fast
        when an entire account is rate-limited (avoids paying the
        per-worker timeout N times for an account that's known to be
        unusable until quota resets). Caller must hold sess.lock so
        the dummy /v1/messages we submit can't be interleaved with a
        real one."""
        try:
            await asyncio.wait_for(
                self._prewarm_bootstrap(sess),
                timeout=self.config.claude.timeouts.prewarm_seconds)
            return True
        except asyncio.TimeoutError:
            log.warning("bootstrap prewarm timed out user=%s; "
                        "first real request may hit rate limit", sess.user_id)
            return False
        except Exception:
            log.exception("bootstrap prewarm failed user=%s; "
                          "first real request may hit rate limit", sess.user_id)
            return False

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
        check_interval = self.config.claude.timeouts.restart_check_interval_seconds
        drain_timeout = self.config.claude.timeouts.restart_drain_seconds
        while True:
            try:
                await asyncio.sleep(check_interval)
                for user_id, sess in list(self.sessions.items()):
                    if sess.age_seconds() <= interval:
                        continue
                    log.info("scheduled restart user=%s age=%.0fs",
                             user_id, sess.age_seconds())
                    async with sess.lock:
                        # Hold the session lock to block new submissions;
                        # wait for any already-streaming responses to
                        # finish before tearing the worker down.
                        deadline = time.monotonic() + drain_timeout
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
