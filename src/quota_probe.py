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
import datetime
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, TYPE_CHECKING

import yaml

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
                 tick_seconds: float = 300.0,
                 cooldown_path: Path | None = None) -> None:
        self.manager = manager
        self.accounts = list(accounts)
        self.tick_seconds = float(tick_seconds)
        # Where to persist active 429 cooldowns. None disables
        # persistence — useful in tests or when /data/proxy isn't
        # mounted RW. With persistence, container rebuild no longer
        # triggers a fresh wave of probes against accounts that are
        # still under Anthropic's penalty cooldown.
        self.cooldown_path = cooldown_path
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
        # Wall-clock twin of `_last_success_mono`. Populated by record()
        # on every successful probe; persisted to the cooldown file so
        # a rebuild within `tick_seconds` of the last success soft-
        # skips that account instead of triggering a fresh /usage burst.
        self._last_success_wall: dict[str, float] = {}
        # Restore any persisted cooldowns from a previous container
        # incarnation. Expired entries are dropped silently. New
        # accounts (not in the file) start with no gate, same as a
        # fresh install.
        self._load_cooldown_state()

    # ── cooldown persistence (survives container restart) ───────────

    def _load_cooldown_state(self) -> None:
        """Restore active gates from disk. Two kinds:

          - `until_unix`: a hard 429 cooldown. Active until that
            wall-clock instant; past entries are dropped silently.
          - `last_success_unix`: a soft tick-aware cooldown. The
            account was last probed successfully at that moment;
            we restore `_last_success_mono` so the periodic tick
            (which gates on `now - last_success >= tick_seconds`)
            soft-skips until the natural 5-min window elapses.

        Monotonic anchors are reconstructed by translating wall-clock
        distance into a future/past monotonic offset, so a clock jump
        backward can't free a cooldown early."""
        if self.cooldown_path is None or not self.cooldown_path.is_file():
            return
        try:
            data = yaml.safe_load(self.cooldown_path.read_text()) or {}
        except (yaml.YAMLError, OSError) as e:
            log.warning("quota cooldown file unreadable (%s); ignoring", e)
            return
        accounts = data.get("accounts") if isinstance(data, dict) else None
        if not isinstance(accounts, dict):
            return
        now_wall = time.time()
        now_mono = time.monotonic()
        restored_hard = 0
        restored_soft = 0
        for name, entry in accounts.items():
            if not isinstance(entry, dict):
                continue
            until_wall = entry.get("until_unix")
            last_success_wall = entry.get("last_success_unix")
            if isinstance(until_wall, (int, float)):
                # Hard cooldown path (429 not yet expired).
                remaining = float(until_wall) - now_wall
                if remaining <= 0:
                    continue
                self._cooldown_until_wall[name] = float(until_wall)
                self._cooldown_until_mono[name] = now_mono + remaining
                self._last_attempt_mono[name] = now_mono
                self._errors[name] = QuotaAttemptError(
                    attempted_at_unix=now_wall,
                    error=(f"已持久化的 429 冷却，仍剩 {int(remaining)}s "
                           f"— 下一次成功探测会清除"),
                    cooldown_until_unix=float(until_wall),
                )
                restored_hard += 1
            elif isinstance(last_success_wall, (int, float)):
                # Soft cooldown: the tick gate compares
                # (now_mono - _last_success_mono) against tick_seconds.
                # Translate the recorded wall time into a synthetic
                # monotonic so the comparison gives the same answer
                # it would have if the process had not restarted.
                elapsed = now_wall - float(last_success_wall)
                if elapsed >= self.tick_seconds or elapsed < 0:
                    # Too old (next tick should fire) or future (clock
                    # jumped) — skip; treat as no prior success.
                    continue
                self._last_success_mono[name] = now_mono - elapsed
                self._last_success_wall[name] = float(last_success_wall)
                # Also bump _last_attempt_mono so the 60s MIN_REFRESH
                # floor blocks an immediate refresh attempt too.
                self._last_attempt_mono[name] = now_mono - elapsed
                restored_soft += 1
        if restored_hard or restored_soft:
            log.info("quota cooldown: restored hard=%d soft=%d from %s",
                     restored_hard, restored_soft, self.cooldown_path)

    def _save_cooldown_state(self) -> None:
        """Atomic-write the current gate state. For each account we
        emit the strongest active gate: 429 cooldowns override
        success windows; entries whose window has elapsed are omitted
        so the file naturally drains. No-op when cooldown_path is None."""
        if self.cooldown_path is None:
            return
        now_wall = time.time()
        out: dict[str, dict[str, float]] = {}
        # Hard 429 cooldowns first — they dominate per record_429's
        # contract (which also pops last_success_wall).
        for name, until in self._cooldown_until_wall.items():
            if until > now_wall:
                out[name] = {"until_unix": float(until)}
        # Soft success windows. Skip accounts already captured by a
        # 429 entry; skip success records older than tick_seconds (the
        # tick would no longer block them, persisting is just noise).
        for name, last_success in self._last_success_wall.items():
            if name in out:
                continue
            if (now_wall - last_success) >= self.tick_seconds:
                continue
            out[name] = {"last_success_unix": float(last_success)}
        payload = {"accounts": out}
        try:
            self.cooldown_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.cooldown_path.with_suffix(
                self.cooldown_path.suffix + ".tmp")
            tmp.write_text(yaml.safe_dump(payload, sort_keys=True))
            tmp.replace(self.cooldown_path)
        except OSError as e:
            log.warning("quota cooldown persist failed: %s", e)

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
        now_mono = time.monotonic()
        self._last_success_mono[account_name] = now_mono
        self._last_attempt_mono[account_name] = now_mono
        # Track wall-clock so we can persist "last successful probe at
        # T" — load() converts it back into a future monotonic skip.
        self._last_success_wall[account_name] = fetched_at_unix
        # Success clears any active 429 cooldown — fresh data trumps a
        # stale "we were rate-limited an hour ago" gate. The persisted
        # state replaces `until_unix` with `last_success_unix` for this
        # account so the next rebuild within tick_seconds soft-skips
        # instead of re-probing.
        self._cooldown_until_mono.pop(account_name, None)
        self._cooldown_until_wall.pop(account_name, None)
        self._save_cooldown_state()
        log.info("quota recorded account=%s 5h=%s%% 7d=%s%%",
                 account_name,
                 (kwargs.get("five_hour") or {}).get("utilization"),
                 (kwargs.get("seven_day") or {}).get("utilization"))
        # Plumb saturated quotas into the routing-block table so
        # pick() avoids accounts that we already know are out of
        # budget — saves a wasted /v1/messages whose only outcome is
        # a 429. Only the overall windows (five_hour / seven_day)
        # block routing: per-model tiers (seven_day_opus / _sonnet)
        # don't, because pick() routes by account, not model.
        self._maybe_mark_from_quota(account_name, kwargs)

    def _maybe_mark_from_quota(self, account_name: str,
                                tiers: dict[str, Any]) -> None:
        """Inspect the per-window utilization. For each overall window
        at >=100% with a future resets_at, mark the account as
        rate-limited until that moment via the manager. If multiple
        windows are saturated, pick the LATEST resets_at — the account
        is blocked until all known limits clear.

        Reset clearing is implicit: mark_account_rate_limited_until
        installs `until=epoch`; manager.account_rate_limit() lazily
        GCs expired entries on read. When the window resets and the
        next probe shows <100%, we simply don't re-mark; the old mark
        expires on its own at the same epoch."""
        candidates: list[tuple[str, float]] = []
        # Mapping of snapshot key → reason token the UI knows how to
        # label (REASON_LABEL in admin.html). Order is informational —
        # we pick the LATEST timestamp regardless.
        WINDOWS = (("five_hour", "5hour_limit"),
                   ("seven_day", "weekly_limit"))
        for key, reason in WINDOWS:
            tier = tiers.get(key)
            if not isinstance(tier, dict):
                continue
            u = tier.get("utilization")
            if not isinstance(u, (int, float)):
                continue
            # API surface has flipped between 0..1 and 0..100 in
            # different shipped CLI versions; normalise defensively.
            norm = u / 100.0 if u > 1 else u
            if norm < 1.0:
                continue
            resets = tier.get("resets_at") or tier.get("resetsAt")
            epoch = _parse_iso8601_epoch(resets)
            if epoch is None or epoch <= time.time():
                continue
            candidates.append((reason, epoch))
        if not candidates:
            return
        # Latest reset wins — multi-limit accounts stay blocked until
        # the longest window clears. mark_account_rate_limited_until
        # overwrites unconditionally, but we never call it with a
        # SHORTER until than what's already there (we always pass the
        # maximum across saturated windows here, and on the next
        # probe we re-evaluate).
        candidates.sort(key=lambda x: x[1], reverse=True)
        reason, epoch = candidates[0]
        if hasattr(self.manager, "mark_account_rate_limited_until"):
            self.manager.mark_account_rate_limited_until(
                account_name, reason, epoch)
            log.warning("quota auto-block account=%s reason=%s until=%s "
                        "(driven by /api/oauth/usage)",
                        account_name, reason,
                        time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime(epoch)))

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
        # 429 dominates any prior soft-cooldown from a past success —
        # the upstream just told us to back off harder, the success
        # window is no longer relevant.
        self._last_success_wall.pop(account_name, None)
        log.warning("quota 429 account=%s backoff=%ss (retry_after=%s)",
                    account_name, int(backoff), retry_after_seconds)
        # Persist so a docker compose restart doesn't reset our memory
        # of the cooldown — without this, every rebuild fires a fresh
        # initial_probe burst that immediately re-429s and the
        # penalty window extends.
        self._save_cooldown_state()

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


def _parse_iso8601_epoch(value: Any) -> float | None:
    """Parse an Anthropic-style ISO 8601 reset timestamp into a unix
    epoch. Returns None for missing / unparseable input. Accepts both
    `Z` suffix and explicit `+00:00` offsets; assumes UTC when no
    timezone info is present (Anthropic always sends one but we tolerate
    drift)."""
    if not isinstance(value, str) or not value:
        return None
    raw = value[:-1] if value.endswith("Z") else value
    try:
        dt = datetime.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.timestamp()
