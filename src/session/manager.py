from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable

from ..config import Config
from ..rate_limit import classify_rate_limit_reason
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
        # Callable set by main.py once QuotaProbeService exists:
        # signature (account_name, body, fetched_at_unix). When a worker
        # emits a quota_usage event, _on_worker_quota looks up the
        # account name and dispatches via this hook. None when the
        # service is disabled (no `accounts:` block — there's no
        # per-account state to keep).
        self.quota_record_cb: Callable[[str, dict, float], None] | None = None
        # Companion for the 429 path. signature:
        # (account_name, retry_after_seconds_or_none, fetched_at_unix).
        self.quota_429_cb: Callable[[str, float | None, float], None] | None = None
        self.sessions: dict[str, ClaudeSession] = {}
        self.lock = asyncio.Lock()
        self._next_port_offset = 0
        self._restarter_task: asyncio.Task | None = None
        # Round-robin tiebreaker counter, keyed by the tuple of pool
        # members. Used only when every worker in the pool is busy.
        self._rr: dict[tuple[str, ...], int] = {}
        # Session→account affinity table. Keyed by a hash of the
        # conversation's stable material (system+tools+first message),
        # value is (account_name, last_used_monotonic). Keeps a
        # conversation's turns on one account so Anthropic's per-account
        # prompt cache stays warm. Lazily expired on read; bulk-pruned
        # when it grows past _SESSION_AFFINITY_MAX.
        self._session_affinity: dict[str, tuple[str, float]] = {}
        # Per-account routing-block table. Keyed by account name from
        # the `accounts:` config block; legacy single-account deployments
        # never populate this. Entries have a `kind` field that the UI
        # uses to distinguish "real rate-limit" from "degraded /
        # unknown failure" — both block routing, but the operator's
        # next move differs (wait for reset vs. investigate).
        self._account_rl: dict[str, AccountIssue] = {}
        # Per-account exponential-backoff streak for the bare-rate_limit
        # (no-reset-header) 429. Value is (streak, last_seen_monotonic).
        # _next_rl_backoff() escalates the cooldown while 429s keep
        # recurring and resets after a healthy gap. See
        # rate_limit_base/max_cooldown_seconds in config.
        self._rl_streak: dict[str, tuple[int, float]] = {}

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

    def account_name_for(self, user_id: str) -> str | None:
        """Public reverse-lookup of a worker's account name."""
        return self._account_name_for(user_id)

    def affinity_account(self, session_key: str) -> str | None:
        """The account a conversation is currently pinned to, IGNORING the
        account's rate-limit state. Used by the wait-and-retry path, which
        deliberately stays on the pinned account through a cooldown rather
        than spreading the limit to a cold-cache account. Honors the TTL
        (returns None if the binding expired) but does NOT create one."""
        entry = self._session_affinity.get(session_key)
        if entry is None:
            return None
        acct, ts = entry
        ttl = self.config.claude.session_affinity_ttl_seconds
        if (time.monotonic() - ts) > ttl:
            self._session_affinity.pop(session_key, None)
            return None
        return acct

    def account_cooldown_remaining(self, account_name: str) -> float:
        """Seconds until the account's rate-limit window expires; 0.0 when
        it is not currently limited (lazily expires the marker on read)."""
        state = self.account_rate_limit(account_name)
        if state is None:
            return 0.0
        return max(0.0, state.until - time.time())

    # Sibling workers on one account can all 429 within a second or two
    # of the same TPM spike. Treat 429s closer together than this as one
    # incident so the streak doesn't jump several steps from a single burst.
    _RL_BURST_DEBOUNCE_SECONDS = 5.0

    # Max workers the scheduled restarter recycles in a single pass. All
    # workers boot together, so they cross restart_interval in the same
    # tick; without a cap the restarter tears down + respawns all of them
    # back-to-back — a thundering herd that spikes fd/CPU use and (under a
    # low nofile limit) trips socketpair. Capping per pass spreads the
    # churn; after the first staggered rollout restarts stay desynchronised
    # because each worker's age now resets at a different tick.
    _RESTART_MAX_PER_PASS = 4

    def mark_account_rate_limited(self, account_name: str, reason: str,
                                  window_seconds: float,
                                  escalate: bool = False) -> None:
        """Record that an account hit a positive rate-limit signal
        (SSE error / TUI modal / 429). See `_mark_account_issue` for
        the merge semantics — short windows don't shrink a longer
        existing window, but a rate_limit observation upgrades a prior
        `degraded` mark (we now know it's quota, not perms).

        When `escalate` is set (the bare no-reset-header 429 — a rolling
        TPM/RPM spike), the cooldown is computed by exponential backoff
        per account instead of using `window_seconds`. Authoritative
        windows (parsed reset / weekly / 5-hour) pass escalate=False so
        their real reset time is honored verbatim."""
        if escalate:
            window_seconds = self._next_rl_backoff(account_name)
        self._mark_account_issue(
            account_name, kind="rate_limit", reason=reason,
            window_seconds=window_seconds)

    def _next_rl_backoff(self, account_name: str) -> float:
        """Compute the next cooldown for a bare-rate_limit 429 on this
        account and advance its streak.

          - gap < debounce  → same incident (sibling worker); reuse streak
          - gap > cap        → account recovered; reset to base (streak 1)
          - otherwise        → consecutive probe re-429'd; double the window

        Window = min(base * 2^(streak-1), cap)."""
        base = float(self.config.claude.rate_limit_base_cooldown_seconds)
        cap = float(self.config.claude.rate_limit_max_cooldown_seconds)
        now = time.monotonic()
        streak, last = self._rl_streak.get(account_name, (0, 0.0))
        gap = now - last
        if gap < self._RL_BURST_DEBOUNCE_SECONDS and streak >= 1:
            pass                       # same burst — don't advance
        elif gap > cap:
            streak = 1                 # recovered — fresh incident
        else:
            streak += 1                # back-to-back — escalate
        streak = max(streak, 1)
        window = min(base * (2 ** (streak - 1)), cap)
        self._rl_streak[account_name] = (streak, now)
        log.info("account=%s rate-limit backoff: streak=%d window=%.0fs",
                 account_name, streak, window)
        return window

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
            until all known limits clear) and recompute `reason` from
            the SURVIVING window via classify_rate_limit_reason. The
            stored reason must match the stored until: if a 4-day
            weekly_limit window is still on the books, a later 5h
            bare-429 backoff must not relabel it as "rate_limit" —
            the panel would then show "rate_limit, remaining 3d22h".
            For rate_limit kind only — degraded reason is opaque
            text from a caller and we keep it as-is.
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
                surviving_until = max(existing.until, new_until)
                if kind == "rate_limit":
                    # Reason must reflect the surviving window, not
                    # whichever event fired last. classify_rate_limit_reason
                    # is the ground-truth mapping (see rate_limit.py:88).
                    surviving_reason = classify_rate_limit_reason(
                        surviving_until - now)
                else:
                    # degraded: caller-supplied free-text reason, latest wins.
                    surviving_reason = reason
                if surviving_reason != existing.reason:
                    log.info("account=%s reason refined: %s -> %s "
                             "(window now %s)",
                             account_name, existing.reason, surviving_reason,
                             time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                           time.gmtime(surviving_until)))
                existing.reason = surviving_reason
                existing.until = surviving_until
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
        Returns True if a marker was removed. Also resets the backoff
        streak — the operator asserting the account is fine means the
        next 429 should start fresh at the base cooldown."""
        self._rl_streak.pop(account_name, None)
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

    def _on_worker_quota(self, user_id: str, body: dict,
                          fetched_at_unix: float) -> None:
        """Forward a /usage probe result to QuotaProbeService keyed by
        account. Best-effort: any failure is logged, never propagates —
        quota tracking is observability."""
        if self.quota_record_cb is None:
            return
        try:
            account = self._account_name_for(user_id)
            if account is None:
                # Legacy single-account mode has no account name — skip.
                return
            self.quota_record_cb(account, body, fetched_at_unix)
        except Exception:
            log.exception("quota record failed user=%s", user_id)

    def _on_worker_quota_429(self, user_id: str,
                              retry_after_seconds: float | None,
                              fetched_at_unix: float) -> None:
        """Forward a /api/oauth/usage 429 to QuotaProbeService keyed by
        account, so the service can put the account into cooldown."""
        if self.quota_429_cb is None:
            return
        try:
            account = self._account_name_for(user_id)
            if account is None:
                return
            self.quota_429_cb(account, retry_after_seconds, fetched_at_unix)
        except Exception:
            log.exception("quota 429 record failed user=%s", user_id)

    async def probe_quota_for_account(self, account_name: str) -> str | None:
        """Pick one idle worker in the account and trigger a /usage probe
        on it. Returns the user_id used, or None if no worker was
        available (all busy, or account has no workers).

        Probing requires holding the worker's session.lock so the /usage
        keystroke doesn't race with a real /v1/messages trigger. To
        avoid waiting on a long-running call, we ONLY pick workers
        whose lock is not currently held — if everyone's busy, this
        tick is skipped and the next 5-min cycle retries."""
        # Find workers belonging to this account.
        candidates: list[ClaudeSession] = []
        for user_id, sess in self.sessions.items():
            if self._account_name_for(user_id) != account_name:
                continue
            if sess.proc is None or sess.proc.returncode is not None:
                continue
            candidates.append(sess)
        if not candidates:
            return None
        # Prefer truly-idle workers (no in-flight + lock not held). Fall
        # back to any worker with no in-flight even if lock briefly held
        # (e.g. by a prewarm cycle). If everyone's busy serving real
        # traffic, skip this tick.
        idle_unlocked = [s for s in candidates
                         if not s._channels and not s.lock.locked()]
        target = idle_unlocked[0] if idle_unlocked else None
        if target is None:
            log.info("quota probe: no idle worker for account=%s "
                     "(workers=%d); skipping this tick",
                     account_name, len(candidates))
            return None
        async with target.lock:
            ok = await target.probe_quota()
        return target.user_id if ok else None

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
                                window_seconds: float,
                                escalate: bool = False) -> None:
        """Callback wired into every ClaudeSession; fires when its
        SSE stream's head contained `rate_limit_error`. Resolves the
        worker → account and marks the account. `escalate` forwards the
        per-account exponential-backoff request for bare no-reset 429s."""
        acc_name = self._account_name_for(user_id)
        if acc_name is None:
            # Legacy single-account mode: no account scope to mark. Log
            # so the operator at least sees the event in the proxy log.
            log.warning("user=%s hit rate_limit_error (%s) — no account "
                        "scope to mark in legacy mode", user_id, reason)
            return
        self.mark_account_rate_limited(
            acc_name, reason, window_seconds, escalate=escalate)

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
                                     on_usage=self._on_worker_usage,
                                     on_quota=self._on_worker_quota)
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

    # How long a session is treated as "TUI cooling down" after its
    # last response channel closed. Keeps pick() from racing the
    # placeholder keystroke against the tail-end of TUI rendering
    # (spinner animations, tool_permission modals, "(1m 0s · ↑ N tokens)"
    # status lines, etc.). 2.0s is long enough to cover one TUI redraw
    # at typical refresh rates and short enough that under sustained
    # load every worker eventually clears the cooldown.
    _PICK_TUI_COOLDOWN_SECONDS = 2.0

    # Cap on the affinity table. Each entry is tiny, but distinct
    # conversations are unbounded over time, so prune past this.
    _SESSION_AFFINITY_MAX = 10000

    async def pick(self, pool: list[str],
                   session_key: str | None = None) -> ClaudeSession:
        """Pick a session from a token's user pool.

        Selection layers (most-preferred first):
          1. Workers whose account is NOT rate-limited.
             If all alternatives are rate-limited we fall back to the
             whole pool — at least one of them needs to handle the
             request, and going through a limited account at least lets
             Anthropic re-confirm whether the limit still applies.
          2. Session affinity (when `session_key` given and the feature
             is on): restrict candidates to the account this conversation
             was last routed to, so Anthropic's per-account prompt cache
             stays warm across turns. Transparent fallback to the full
             usable set when that account is rate-limited / absent.
          3. Cool idle — _channels empty AND last close > cooldown.
          4. Warm idle — _channels empty but in cooldown window.
          5. Fewest-in-flight, RR-tied so the load spreads instead of
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

        # Session affinity: narrow to one account's workers when this
        # conversation already has a (still-valid, not-rate-limited)
        # binding. Selection below then spreads across THAT account's
        # workers; cross-conversation spreading is preserved because
        # different session_keys bind to different accounts.
        candidates = usable
        use_affinity = (session_key is not None
                        and self.config.claude.session_affinity)
        if use_affinity:
            acct = self._resolve_affinity(session_key)
            if acct is not None:
                same_acct = [s for s in usable
                             if self._account_name_for(s.user_id) == acct]
                if same_acct:
                    candidates = same_acct

        chosen = self._select_within(candidates)

        if use_affinity:
            acct = self._account_name_for(chosen.user_id)
            if acct is not None:
                self._record_affinity(session_key, acct)
        return chosen

    def _select_within(self, usable: list[ClaudeSession]) -> ClaudeSession:
        """Apply the idle/busy selection tiers to a candidate set and
        return one session. RR-keyed by the candidate user_ids so the
        rotation is stable per (sub)pool."""
        now = time.monotonic()
        cooldown = self._PICK_TUI_COOLDOWN_SECONDS

        def _pty_busy(s: ClaudeSession) -> bool:
            # Cross-channel idleness: even after our SSE channel closes,
            # claude CLI may still be running the tool chain triggered
            # by tool_use blocks in the response (Task subagents, Read,
            # Bash, etc) — visible as "esc to interrupt" footer or as
            # a tool-permission modal on screen_tail. Sending the next
            # "say hi" placeholder into a TUI in that state queues the
            # keystroke without firing /v1/messages, so the next request
            # eats a 90s mitm-intercept timeout.
            pty = getattr(s, "pty", None)
            if pty is None:
                return False
            try:
                return pty.is_tui_busy()
            except Exception:
                return False

        def _is_cool_idle(s: ClaudeSession) -> bool:
            if s._channels:
                return False
            t = s._last_channel_close_at
            if t is not None and (now - t) < cooldown:
                return False
            return not _pty_busy(s)

        key = tuple(s.user_id for s in usable)
        cool_idle = [s for s in usable if _is_cool_idle(s)]
        if cool_idle:
            idx = self._rr.get(key, 0) % len(cool_idle)
            self._rr[key] = idx + 1
            return cool_idle[idx]

        # No cool worker available — prefer a warm-idle one over an
        # actually-busy one. Warm idle still has _channels empty but is
        # within the TUI cooldown window OR has a busy PTY; trigger()
        # will pre-dismiss any leftover modal, and the 80ms wait there
        # covers the most common race. This branch only fires under
        # sustained burst load where every worker has just finished a
        # request — picking a warm-idle is still better than piling
        # onto an in-flight one. We DO bias against pty-busy workers
        # within this branch when there's a not-busy alternative.
        warm_idle_all = [s for s in usable if not s._channels]
        warm_idle_quiet = [s for s in warm_idle_all if not _pty_busy(s)]
        warm_idle = warm_idle_quiet or warm_idle_all
        if warm_idle:
            idx = self._rr.get(key, 0) % len(warm_idle)
            self._rr[key] = idx + 1
            return warm_idle[idx]

        # All busy — fewest in-flight wins, RR breaks ties.
        min_inflight = min(len(s._channels) for s in usable)
        candidates = [s for s in usable if len(s._channels) == min_inflight]
        idx = self._rr.get(key, 0) % len(candidates)
        self._rr[key] = idx + 1
        return candidates[idx]

    def _resolve_affinity(self, session_key: str) -> str | None:
        """Return the bound account for this conversation, or None if no
        binding exists, it has expired, or the account is currently
        rate-limited (in which case the caller spreads normally and
        rebinds to wherever the request lands)."""
        entry = self._session_affinity.get(session_key)
        if entry is None:
            return None
        acct, ts = entry
        ttl = self.config.claude.session_affinity_ttl_seconds
        if (time.monotonic() - ts) > ttl:
            self._session_affinity.pop(session_key, None)
            return None
        if self.is_account_rate_limited(acct):
            return None
        return acct

    def _record_affinity(self, session_key: str, account_name: str) -> None:
        """Bind (or refresh) this conversation to an account. Refreshing
        the timestamp on every turn keeps an active conversation pinned;
        idle ones age out via _resolve_affinity's TTL check."""
        self._session_affinity[session_key] = (account_name, time.monotonic())
        if len(self._session_affinity) > self._SESSION_AFFINITY_MAX:
            self._prune_affinity()

    def _prune_affinity(self) -> None:
        """Drop expired bindings; if still over cap, drop the oldest."""
        ttl = self.config.claude.session_affinity_ttl_seconds
        now = time.monotonic()
        self._session_affinity = {
            k: v for k, v in self._session_affinity.items()
            if (now - v[1]) <= ttl
        }
        if len(self._session_affinity) > self._SESSION_AFFINITY_MAX:
            keep = sorted(self._session_affinity.items(),
                          key=lambda kv: kv[1][1],
                          reverse=True)[:self._SESSION_AFFINITY_MAX]
            self._session_affinity = dict(keep)

    def _is_session_account_rate_limited(self, sess: ClaudeSession) -> bool:
        acc_name = self._account_name_for(sess.user_id)
        if acc_name is None:
            return False
        return self.is_account_rate_limited(acc_name)

    async def pick_excluding(self, pool: list[str],
                             exclude_user_ids: set[str]) -> ClaudeSession | None:
        """Variant of pick() used by hedged retry: pick a worker from
        the pool that is NOT in `exclude_user_ids`. Returns None if no
        such worker exists (pool size 1, or all alternatives excluded).

        Uses the same selection layers as pick() so the backup worker
        is still preferred to be idle / not on a rate-limited account.
        Falls back through the same warm-idle and fewest-in-flight
        tiers when no truly-idle alternative exists."""
        filtered_pool = [u for u in pool if u not in exclude_user_ids]
        if not filtered_pool:
            return None
        return await self.pick(filtered_pool)

    async def spawn_account(self, account_name: str,
                              background_rest: bool = False) -> list[str]:
        """Spawn every worker for an account that was just added to
        `self.config.accounts`. Mirrors the per-account chain logic from
        `start()` (serial spawn + prewarm within the account, so the
        per-OAuth bootstrap burst doesn't trip Anthropic's rate limiter).

        Returns the list of user_ids that ended up successfully
        registered in self.sessions. Already-existing sessions are
        skipped (idempotent — safe to call after a partial failure).

        `background_rest`: when True (used by the dashboard's add-
        account flow), only the FIRST worker is spawned + prewarmed
        synchronously. The remaining workers run in a fire-and-forget
        asyncio.Task so the HTTP request returns in ~10s instead of
        ~50s for a 5-worker account. The first sync spawn still
        validates credentials end-to-end — if it fails, the caller
        gets an exception and the rest never get scheduled. The
        background workers hold their session locks for the duration
        of their own spawn, so real requests that pick those user_ids
        wait until each worker is actually ready.

        Caller's responsibility: make sure `self.config.users` has a
        pool referencing the new account's user_ids, otherwise the
        workers will be alive but unrouted. /admin/accounts/new wires
        this automatically; manual edits via /admin/reload go through
        `_apply_accounts_diff` which does the same."""
        acc = self.config.accounts.get(account_name)
        if acc is None:
            raise ValueError(f"account {account_name!r} not in config.accounts")
        user_ids = [f"{account_name}-{i}" for i in range(acc.workers)]

        # Allocate ports + register placeholder sessions under self.lock.
        # Same single-pass approach as start() so each chain's spawn
        # runs outside the lock (parallelism not blocked by lock contention).
        chain: list[ClaudeSession] = []
        async with self.lock:
            for uid in user_ids:
                if uid in self.sessions:
                    continue
                port = self.config.mitm.port_base + self._next_port_offset
                self._next_port_offset += 1
                sess = ClaudeSession(user_id=uid, mitm_port=port,
                                     config=self.config,
                                     on_rate_limit=self._on_worker_rate_limited,
                                     on_usage=self._on_worker_usage,
                                     on_quota=self._on_worker_quota,
                                     on_quota_429=self._on_worker_quota_429)
                self.sessions[uid] = sess
                await sess.lock.acquire()
                chain.append(sess)

        async def _spawn_one(sess: ClaudeSession) -> bool:
            try:
                await sess.start()
                try:
                    ok = await self._safe_prewarm(sess)
                    if ok:
                        log.info("spawn_account: prewarmed user=%s",
                                 sess.user_id)
                finally:
                    if sess.lock.locked():
                        sess.lock.release()
                return True
            except Exception:
                log.exception("spawn_account: spawn failed user=%s; "
                              "dropping session", sess.user_id)
                self.sessions.pop(sess.user_id, None)
                if sess.lock.locked():
                    sess.lock.release()
                return False

        if not chain:
            log.info("spawn_account account=%s spawned=0/0 (no new workers)",
                     account_name)
            return []

        # Synchronous: first worker. Doubles as credentials validation
        # — if claude CLI can't read .credentials.json or fails OAuth
        # bootstrap, this raises and the caller sees the failure
        # without us having scheduled background work that will also
        # fail in the same way.
        first, rest = chain[0], chain[1:]
        spawned: list[str] = []
        if await _spawn_one(first):
            spawned.append(first.user_id)

        if not rest:
            log.info("spawn_account account=%s spawned=%d/%d",
                     account_name, len(spawned), len(user_ids))
            return spawned

        if background_rest:
            async def _spawn_rest() -> None:
                # Serial within an account (same per-OAuth bootstrap-
                # burst rationale as start()). Each iteration awaits
                # the previous before kicking off the next.
                bg_spawned = 0
                for sess in rest:
                    if await _spawn_one(sess):
                        bg_spawned += 1
                log.info("spawn_account background account=%s "
                         "spawned=%d/%d (foreground=%d, total=%d/%d)",
                         account_name, bg_spawned, len(rest),
                         len(spawned), bg_spawned + len(spawned),
                         len(user_ids))
            # Fire-and-forget: exceptions are logged in _spawn_one,
            # nothing else needs the result.
            asyncio.create_task(_spawn_rest())
            log.info("spawn_account account=%s spawned=%d/%d "
                     "(rest %d in background)",
                     account_name, len(spawned), len(user_ids), len(rest))
            return spawned

        for sess in rest:
            if await _spawn_one(sess):
                spawned.append(sess.user_id)
        log.info("spawn_account account=%s spawned=%d/%d",
                 account_name, len(spawned), len(user_ids))
        return spawned

    async def stop_account(self, account_name: str,
                            drain_seconds: float = 30.0) -> int:
        """Drain + stop every worker belonging to an account, then
        remove them from self.sessions. Returns the number of workers
        actually stopped. Best-effort: a worker that's mid-stream
        gets up to `drain_seconds` for its current /v1/messages to
        complete; after that the subprocess is killed.

        Also clears any account-level routing block — once workers
        are gone there's nothing to block. Caller should remove the
        account from self.config.accounts AFTER this returns, plus
        prune any user pools that referenced its user_ids."""
        targets: list[ClaudeSession] = []
        for uid, sess in list(self.sessions.items()):
            if self._account_name_for(uid) == account_name:
                targets.append(sess)

        async def _drain_and_stop(sess: ClaudeSession) -> None:
            try:
                # Wait for in-flight requests to finish. The session
                # lock is held while one is in flight; acquire it
                # (with timeout) so we don't truncate a streaming
                # response mid-token.
                try:
                    await asyncio.wait_for(sess.lock.acquire(),
                                            timeout=drain_seconds)
                    sess.lock.release()
                except asyncio.TimeoutError:
                    log.warning("stop_account: drain timed out for "
                                "user=%s after %.0fs; killing anyway",
                                sess.user_id, drain_seconds)
                await sess.stop()
            except Exception:
                log.exception("stop_account: error stopping user=%s",
                              sess.user_id)
            finally:
                self.sessions.pop(sess.user_id, None)

        await asyncio.gather(*[_drain_and_stop(s) for s in targets])
        # Clear any routing block — workers gone, no point keeping
        # the block (or its expiry) around.
        self._account_rl.pop(account_name, None)
        log.info("stop_account account=%s stopped=%d",
                 account_name, len(targets))
        return len(targets)

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
                                         on_usage=self._on_worker_usage,
                                         on_quota=self._on_worker_quota,
                                         on_quota_429=self._on_worker_quota_429)
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
                restarted_this_pass = 0
                for user_id, sess in list(self.sessions.items()):
                    if sess.age_seconds() <= interval:
                        continue
                    if restarted_this_pass >= self._RESTART_MAX_PER_PASS:
                        # Defer the rest to later passes so we don't
                        # recycle every same-age worker in one burst.
                        log.info("restarter: hit per-pass cap (%d); deferring "
                                 "remaining overdue workers to next pass",
                                 self._RESTART_MAX_PER_PASS)
                        break
                    restarted_this_pass += 1
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
