"""Per-account subscription quota tracker.

Triggers each account's idle worker to type `/usage` into its claude
TUI on a periodic tick (default 5 min). The TUI's HTTP call to
`/api/oauth/usage` is captured by the mitm addon (src/mitm/addon.py)
and the parsed JSON body lands here via SessionManager.quota_record_cb.

Why go through the worker instead of a direct httpx call?
  - Same wire fingerprint as legitimate CLI traffic — every header
    (User-Agent, x-stainless-*, full anthropic-beta token list, etc.)
    is whatever the pinned claude code CLI version sends. Anthropic
    cannot distinguish a probe from a real `/usage` invocation by an
    operator typing it into a live CLI session.
  - Sidesteps the maintenance burden of mirroring claude code's
    request shape: when the CLI version bumps and the header set
    changes, our probe automatically follows along.

Trade-offs: each probe briefly holds one worker's session.lock (so a
real /v1/messages can't race the /usage keystroke), and depends on the
TUI processing /usage without confirmation modals (it does, as of
CLI 2.1.139). Failure modes (TUI swallows the keystroke, network
error, upstream 429) leave the previous snapshot intact and record
the error separately — dashboard never flips to "no data" because of
one bad tick.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .session.manager import SessionManager

log = logging.getLogger(__name__)


# Minimum seconds between two successful probes for the same account.
# Defends against an operator manually hitting /admin/quotas/refresh in
# a loop, or two ticks landing close together because the previous one
# was delayed. Anthropic's `/api/oauth/usage` is undocumented; this is
# a respectful baseline.
MIN_REFRESH_INTERVAL_SECONDS = 60.0

# Fallback cooldown when the upstream 429 doesn't carry a retry-after
# header. Observed value in the wild is 3600s (1 call/hour/account);
# keep the default conservative so a probe that has no header info
# doesn't immediately re-burn an account's allowance.
DEFAULT_429_BACKOFF_SECONDS = 3600.0


@dataclass
class QuotaSnapshot:
    """One successful /api/oauth/usage capture. Stored per account.
    Errors don't overwrite this — the dashboard keeps showing the last
    known good value even when the latest probe failed."""
    fetched_at_unix: float
    five_hour: dict[str, Any] | None = None
    seven_day: dict[str, Any] | None = None
    seven_day_opus: dict[str, Any] | None = None
    seven_day_sonnet: dict[str, Any] | None = None
    # Future-proofing: any other top-level dict with a `utilization`
    # field that Anthropic adds (e.g. a new model tier) lands here. We
    # surface them on the UI side as additional rows.
    extra_tiers: dict[str, Any] = field(default_factory=dict)


@dataclass
class QuotaAttemptError:
    """Last failure (probe never landed, or landed but body was
    unparseable). Cleared on the next success."""
    attempted_at_unix: float
    error: str
    # When the upstream returned 429, the absolute epoch the cooldown
    # ends. None means the failure wasn't a rate-limit (e.g. no worker
    # available, parse error). UI shows a "下一次刷新: HH:MM" badge
    # when this is populated.
    cooldown_until_unix: float | None = None


class QuotaProbeService:
    """One instance, holds per-account state. Wired into:
      - SessionManager.quota_record_cb  → .record(account, body, ts)
      - Background asyncio task         → .run()
      - GET /admin/quotas               → .state_dict()"""

    def __init__(self, manager: "SessionManager", *,
                 accounts: list[str],
                 tick_seconds: float = 300.0) -> None:
        self.manager = manager
        self.accounts = list(accounts)
        self.tick_seconds = float(tick_seconds)
        self._snapshots: dict[str, QuotaSnapshot] = {}
        self._errors: dict[str, QuotaAttemptError] = {}
        # monotonic clock — defends against system clock jumps for
        # rate-limit gating. Wall-clock fetched_at_unix in the snapshot
        # is for UI display only.
        self._last_success_mono: dict[str, float] = {}
        # last attempt (success or fail), used to skip too-frequent
        # /admin/quotas/refresh button presses.
        self._last_attempt_mono: dict[str, float] = {}
        # Cooldown-until (monotonic, populated by 429). Set ≥ now means
        # skip probes for this account; clears on the next success.
        # We track BOTH monotonic (gating decisions) and wall-clock
        # (UI display) so a clock jump doesn't free a cooldown early.
        self._cooldown_until_mono: dict[str, float] = {}
        self._cooldown_until_wall: dict[str, float] = {}

    # ── ingress: called from SessionManager._on_worker_quota ─────────

    def record(self, account_name: str, body: dict,
                fetched_at_unix: float) -> None:
        """Called by SessionManager when a worker's mitm captures a
        /api/oauth/usage response. Parses the body, updates the
        snapshot, clears any prior error."""
        known = ("five_hour", "seven_day", "seven_day_opus",
                 "seven_day_sonnet")
        kwargs: dict[str, Any] = {k: body.get(k) for k in known}
        extra_tiers: dict[str, Any] = {}
        for k, v in body.items():
            if k in known:
                continue
            # Heuristic: only keep dict entries that carry a
            # `utilization` field — these look like new windows in the
            # same shape. Skip everything else so the snapshot doesn't
            # become a junk drawer.
            if isinstance(v, dict) and "utilization" in v:
                extra_tiers[k] = v
        self._snapshots[account_name] = QuotaSnapshot(
            fetched_at_unix=fetched_at_unix,
            extra_tiers=extra_tiers,
            **kwargs,
        )
        self._errors.pop(account_name, None)
        self._last_success_mono[account_name] = time.monotonic()
        self._last_attempt_mono[account_name] = time.monotonic()
        # Success clears any active cooldown — fresh data trumps a
        # stale "we were rate-limited an hour ago" gate.
        self._cooldown_until_mono.pop(account_name, None)
        self._cooldown_until_wall.pop(account_name, None)
        log.info("quota recorded account=%s 5h=%s%% 7d=%s%%",
                 account_name,
                 (kwargs.get("five_hour") or {}).get("utilization"),
                 (kwargs.get("seven_day") or {}).get("utilization"))

    def record_429(self, account_name: str,
                    retry_after_seconds: float | None,
                    fetched_at_unix: float) -> None:
        """Called when a probe hit upstream HTTP 429. Sets a cooldown
        per retry-after (or DEFAULT_429_BACKOFF_SECONDS when absent) so
        subsequent ticks skip this account until the window clears.
        Does NOT overwrite the prior snapshot."""
        backoff = (float(retry_after_seconds)
                   if retry_after_seconds is not None and retry_after_seconds > 0
                   else DEFAULT_429_BACKOFF_SECONDS)
        self._cooldown_until_mono[account_name] = time.monotonic() + backoff
        self._cooldown_until_wall[account_name] = fetched_at_unix + backoff
        self._errors[account_name] = QuotaAttemptError(
            attempted_at_unix=fetched_at_unix,
            error=(f"Anthropic 限速 HTTP 429 — 已 backoff "
                   f"{int(backoff)} 秒（"
                   f"{'按 retry-after' if retry_after_seconds else '默认'}"
                   f"）"),
            cooldown_until_unix=fetched_at_unix + backoff,
        )
        self._last_attempt_mono[account_name] = time.monotonic()
        log.warning("quota 429 account=%s backoff=%ss (retry_after=%s)",
                    account_name, int(backoff), retry_after_seconds)

    # ── egress: dashboard reads via /admin/quotas ────────────────────

    def state_dict(self) -> dict[str, Any]:
        """Snapshot the current per-account state. Returns
        {accounts: {<name>: {snapshot, last_error, age_seconds, ...}, ...},
         tick_seconds: <int>}."""
        now_mono = time.monotonic()
        now_wall = time.time()
        out: dict[str, Any] = {}
        for name in self.accounts:
            snap = self._snapshots.get(name)
            err = self._errors.get(name)
            last_success = self._last_success_mono.get(name)
            cd_mono = self._cooldown_until_mono.get(name, 0.0)
            cd_wall = self._cooldown_until_wall.get(name)
            cooldown_remaining = max(0, round(cd_mono - now_mono))
            out[name] = {
                "snapshot": asdict(snap) if snap is not None else None,
                "last_error": asdict(err) if err is not None else None,
                # Wall-clock seconds since the snapshot was captured;
                # the UI uses it to render "刷新于 X 秒前" / stale badge.
                "age_seconds": (round(now_wall - snap.fetched_at_unix)
                                if snap is not None else None),
                # Hint for the UI: when the NEXT tick will fire for
                # this account. Negative means it's already due.
                "seconds_until_next_tick": (
                    round(self.tick_seconds - (now_mono - last_success))
                    if last_success is not None else 0),
                # Active 429 cooldown. UI uses cooldown_until_unix to
                # show a "下一次刷新: HH:MM:SS" badge instead of "5 min".
                "cooldown_seconds_remaining": cooldown_remaining,
                "cooldown_until_unix": cd_wall if cd_wall and cooldown_remaining > 0 else None,
            }
        return {
            "accounts": out,
            "tick_seconds": int(self.tick_seconds),
        }

    # ── background tick ─────────────────────────────────────────────

    async def initial_probe(self) -> None:
        """Trigger one probe per account at startup so the dashboard
        has data BEFORE the first tick_seconds window elapses. Serial
        (one at a time) — same as the periodic tick — to keep the
        outbound /api/oauth/usage pattern looking like a human casually
        switching between sessions, not a sync burst."""
        for name in self.accounts:
            await self._probe_one(name)
            # Small stagger between accounts so two consecutive probes
            # don't race the mitm event-loop landing.
            await asyncio.sleep(2.0)

    async def run(self) -> None:
        """Periodic tick. Iterates accounts serially every
        tick_seconds. Designed to be spawned as an asyncio.Task that
        runs for the FastAPI app's lifetime."""
        # First tick fires after `tick_seconds` because initial_probe
        # has already populated the data. Skipping a leading sleep
        # would re-probe immediately, doubling startup load.
        while True:
            try:
                await asyncio.sleep(self.tick_seconds)
                for name in self.accounts:
                    await self._probe_one(name)
                    await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("quota probe tick crashed; "
                              "continuing after the next sleep")

    async def _probe_one(self, account_name: str) -> None:
        """Trigger a probe on one account. Records "no idle worker"
        as a non-overwriting error — the snapshot stays intact."""
        # 60s server-side floor. The periodic tick is much longer
        # (300s default) but a future /admin/quotas/refresh endpoint
        # could call _probe_one directly; this protects upstream.
        last = self._last_attempt_mono.get(account_name, 0.0)
        if (time.monotonic() - last) < MIN_REFRESH_INTERVAL_SECONDS:
            log.debug("quota probe: skipping account=%s (within %ss floor)",
                      account_name, MIN_REFRESH_INTERVAL_SECONDS)
            return
        # 429 cooldown gate. /api/oauth/usage typically returns
        # retry-after: 3600 — probing inside that window just burns
        # another TUI interrupt for another 429. Skip silently; the
        # cooldown is surfaced on /admin/quotas so the operator sees
        # why nothing updated.
        cd = self._cooldown_until_mono.get(account_name, 0.0)
        if cd > time.monotonic():
            log.debug("quota probe: account=%s in cooldown for another %ds",
                      account_name, int(cd - time.monotonic()))
            return
        self._last_attempt_mono[account_name] = time.monotonic()
        try:
            worker = await self.manager.probe_quota_for_account(account_name)
        except Exception as e:
            log.exception("quota probe: probe_quota_for_account raised")
            self._errors[account_name] = QuotaAttemptError(
                attempted_at_unix=time.time(),
                error=f"probe_quota_for_account: {type(e).__name__}: {e}",
            )
            return
        if worker is None:
            self._errors[account_name] = QuotaAttemptError(
                attempted_at_unix=time.time(),
                error="no idle worker available for probe",
            )
            return
        log.info("quota probe: account=%s worker=%s sent /usage",
                 account_name, worker)
        # The result lands asynchronously via .record() when mitm
        # captures the /api/oauth/usage response. Nothing more to do
        # here — _last_attempt_mono is set, the snapshot will update
        # whenever the body arrives (typically <1s).
