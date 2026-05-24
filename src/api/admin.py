"""Admin endpoints — config hot-reload + per-worker control.

Auth model: reuses the tenant auth_dep (any configured API key works),
same as /status. The risk surface here is bigger (mutates running
state), but every legitimate admin already has a key; introducing a
separate admin token would just be an extra thing to lose. If you
deploy to a multi-tenant environment where some keys must NOT have
admin rights, swap this for a stricter dependency.

Hot-reload covers the fields whose live mutation is safe:
  claude.restart_interval_seconds, claude.timeouts.*, oauth_refresh.*,
  users.
Everything else (listen_host/port, mitm.*, claude.binary/home_template)
requires a container restart; /admin/reload returns those as warnings
rather than silently dropping them.
"""
from __future__ import annotations

import asyncio
import logging
import time
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from ..config import Config
from ..oauth_refresh import OAuthRefresher
from ..rate_limit import parse_reset_time
from ..session.manager import SessionManager
from ..usage import UsageStore, estimate_usd

log = logging.getLogger(__name__)


# Fields that mutation-in-place handles correctly. Anything not in this
# set + not in REQUIRES_RESTART is silently ignored on reload (which
# would be a bug — keep these two sets exhaustive vs. the Config schema).
_HOT_RELOADABLE = {
    "claude.restart_interval_seconds",
    "claude.timeouts.mitm_intercept_seconds",
    "claude.timeouts.status_stall_seconds",
    "claude.timeouts.response_stall_seconds",
    "claude.timeouts.restart_drain_seconds",
    "claude.timeouts.worker_ready_seconds",
    "claude.timeouts.prewarm_seconds",
    "claude.timeouts.restart_check_interval_seconds",
    "oauth_refresh.enabled",
    "oauth_refresh.check_interval_seconds",
    "oauth_refresh.refresh_when_expires_within_seconds",
    "users",
}

_REQUIRES_RESTART = {
    "listen_host",
    "listen_port",
    "mitm.port_base",
    "mitm.ca_cert",
    "claude.binary",
    "claude.home_template",
}


def build_router(
    manager: SessionManager,
    config: Config,
    config_path: str | None,
    auth_dep,
    refresher_state: SimpleNamespace,
    credentials_paths,
    usage_store: UsageStore | None = None,
    usage_disabled_reason: str | None = None,
) -> APIRouter:
    """Build the admin router.

    refresher_state.refresher and refresher_state.task are read AND
    mutated by /admin/reload when oauth_refresh.enabled flips. main.py
    is the other writer (sets them up in lifespan); this is single-
    threaded asyncio so no lock is needed."""
    router = APIRouter(prefix="/admin")

    @router.post("/reload")
    async def reload(_pool: list[str] = Depends(auth_dep)) -> dict[str, Any]:
        """Re-read config.yaml, apply hot-reloadable changes in place,
        warn about non-hot-reloadable ones. Returns a structured diff."""
        if config_path is None:
            raise HTTPException(503,
                "config_path not known to this process; "
                "/admin/reload unavailable")
        try:
            new_config = Config.load(config_path)
        except Exception as e:
            raise HTTPException(400, f"config reload failed: {e}")

        changes: list[str] = []
        warnings: list[str] = []

        # --- non-hot-reloadable ---
        for path in _REQUIRES_RESTART:
            old = _get_path(config, path)
            new = _get_path(new_config, path)
            if old != new:
                warnings.append(
                    f"{path}: {old!r} -> {new!r} (ignored — requires container restart)")

        # --- claude.restart_interval_seconds ---
        if config.claude.restart_interval_seconds != new_config.claude.restart_interval_seconds:
            old = config.claude.restart_interval_seconds
            config.claude.restart_interval_seconds = new_config.claude.restart_interval_seconds
            changes.append(
                f"claude.restart_interval_seconds: {old} -> {config.claude.restart_interval_seconds}")

        # --- claude.timeouts.* ---
        for field in new_config.claude.timeouts.model_fields:
            old = getattr(config.claude.timeouts, field)
            new = getattr(new_config.claude.timeouts, field)
            if old != new:
                setattr(config.claude.timeouts, field, new)
                # These two are baked into the worker subprocess's argv
                # at spawn time, so reload only affects future spawns.
                # Operator can force immediate effect via
                # POST /admin/workers/{user_id}/restart.
                note = (" (next worker spawn only)"
                        if field in {"mitm_intercept_seconds",
                                     "response_stall_seconds"} else "")
                changes.append(f"claude.timeouts.{field}: {old} -> {new}{note}")

        # --- oauth_refresh.* ---
        await _apply_oauth_refresh(
            config, new_config, refresher_state, credentials_paths,
            changes, warnings)

        # --- users (add / remove) ---
        await _apply_users_diff(config, new_config, manager, changes)

        log.info("/admin/reload applied: %d changes, %d warnings",
                 len(changes), len(warnings))
        return {"ok": True, "changes": changes, "warnings": warnings}

    @router.post("/workers/{user_id}/restart")
    async def restart_worker(user_id: str,
                             _pool: list[str] = Depends(auth_dep)) -> dict[str, Any]:
        """Tear down and re-create one worker in place, same mitm port.
        Drains in-flight up to restart_drain_seconds before forcing.
        Blocks until the new worker is ready (and prewarmed). Useful for
        manually recycling a stuck worker without waiting for the next
        scheduled restart."""
        sess = manager.sessions.get(user_id)
        if sess is None:
            raise HTTPException(404,
                f"no worker for user_id={user_id!r}; "
                "configured users: {}".format(
                    sorted(set(u for pool in config.users.values() for u in pool))))
        old_age = sess.age_seconds()
        drain_timeout = config.claude.timeouts.restart_drain_seconds
        deadline = time.monotonic() + drain_timeout
        while sess._channels and time.monotonic() < deadline:
            await asyncio.sleep(0.5)
        forced = bool(sess._channels)
        async with sess.lock:
            try:
                await sess.restart()
                await manager._safe_prewarm(sess)
            except Exception as e:
                log.exception("/admin/workers/%s/restart failed", user_id)
                raise HTTPException(500, f"restart failed: {e}")
        log.info("/admin/workers/%s/restart complete (old_age=%.0fs forced=%s)",
                 user_id, old_age, forced)
        return {
            "ok": True,
            "user_id": user_id,
            "old_age_seconds": round(old_age, 1),
            "forced": forced,
        }

    @router.post("/accounts/{account_name}/set-rate-limit")
    async def set_account_rate_limit(
        account_name: str,
        body: dict = Body(...),
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Manually set an account's rate-limit reset time. The operator's
        input is authoritative — useful when claude TUI doesn't surface
        the modal on the worker's PTY (so automatic detection can't find
        it) but the operator has seen the reset time in their own claude
        CLI session and wants to inform the proxy.

        Body accepts either form:
          {"reset_at": "2026-05-27T00:00:00Z"}           # ISO 8601
          {"reset_at": "May 27, 12am UTC"}               # claude TUI wording
          {"reset_at_epoch": 1779840000, "reason": "weekly_limit"}  # explicit

        `reason` defaults to "weekly_limit" (matches the common case)."""
        if account_name not in config.accounts:
            raise HTTPException(404,
                f"unknown account {account_name!r}; configured: "
                f"{sorted(config.accounts.keys())}")
        reason = (body.get("reason") or "weekly_limit").strip() or "weekly_limit"

        epoch = body.get("reset_at_epoch")
        if epoch is None:
            text = body.get("reset_at")
            if not isinstance(text, str) or not text.strip():
                raise HTTPException(400,
                    "body must include either `reset_at` (string, "
                    "ISO 8601 or claude TUI wording) or `reset_at_epoch` "
                    "(number, Unix seconds)")
            epoch = parse_reset_time(text)
            if epoch is None:
                raise HTTPException(400,
                    f"could not parse reset time from {text!r}; "
                    "use ISO 8601 (e.g. \"2026-05-27T00:00:00Z\") or the "
                    "TUI wording (e.g. \"resets May 27, 12am UTC\")")

        try:
            epoch_f = float(epoch)
        except (TypeError, ValueError):
            raise HTTPException(400,
                f"reset_at_epoch must be a number, got {epoch!r}")

        manager.mark_account_rate_limited_until(
            account_name, reason, epoch_f)
        log.info("/admin/accounts/%s/set-rate-limit reason=%s until=%s",
                 account_name, reason,
                 time.strftime("%Y-%m-%dT%H:%M:%SZ",
                               time.gmtime(epoch_f)))
        return {
            "ok": True,
            "account": account_name,
            "reason": reason,
            "until_epoch": epoch_f,
        }

    @router.post("/accounts/{account_name}/clear-rate-limit")
    async def clear_account_rate_limit(
        account_name: str,
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Manually drop an account's rate-limit marker. Useful when the
        operator knows the limit has cleared (e.g. weekly window reset
        on their watch) and doesn't want to wait for the automatic
        expiry. Next routed request will go through and confirm.

        Returns {"cleared": true} if a marker was removed,
        {"cleared": false} if none was set. 404 if the account name is
        not configured."""
        if account_name not in config.accounts:
            raise HTTPException(404,
                f"unknown account {account_name!r}; configured: "
                f"{sorted(config.accounts.keys())}")
        cleared = manager.clear_account_rate_limit(account_name)
        log.info("/admin/accounts/%s/clear-rate-limit cleared=%s",
                 account_name, cleared)
        return {"ok": True, "account": account_name, "cleared": cleared}

    @router.post("/refresh-now")
    async def refresh_now(_pool: list[str] = Depends(auth_dep)) -> dict[str, Any]:
        """Force an immediate OAuth refresh across every configured
        account, bypassing the periodic loop's sleep. Useful when you
        suspect on-disk tokens are stale (e.g., you just hit 401s) and
        don't want to wait for the next check_interval tick.

        Returns:
          - `result`:  aggregated status — "refreshed" if any account
            actually refreshed, otherwise "not_needed", "failed" if any
            failed. Useful for UIs that want a one-glance verdict.
          - `details`: full {path: status} map from the refresher."""
        if refresher_state.refresher is None:
            raise HTTPException(503,
                "oauth_refresh is disabled (oauth_refresh.enabled=false)")
        try:
            details = await refresher_state.refresher.refresh_now()
        except Exception as e:
            log.exception("/admin/refresh-now failed")
            raise HTTPException(500, f"refresh failed: {e}")
        statuses = set(details.values())
        if "failed" in statuses:
            aggregate = "failed"
        elif "refreshed" in statuses:
            aggregate = "refreshed"
        else:
            aggregate = "not_needed"
        return {"ok": True, "result": aggregate, "details": details}

    @router.get("/usage")
    async def get_usage(
        range: str = "today",
        group_by: str = "account",
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Token-usage aggregates.

        Query params:
          range:    lifecycle | today | 7d
                    - "lifecycle" filters by ts >= the earliest live
                      worker's started_at (per group bucket).
                    - "today" filters by ts >= local-day-midnight UTC.
                    - "7d" filters by ts >= now - 7 days.
          group_by: account | worker | litellm_user

        Returns one row per bucket with summed token counters and the
        published-API USD equivalent (subscription accounts don't pay
        per token — the figure is a workload sizing reference, not
        actual spend). Each row also carries `by_model[]` so the UI
        can expand a row to see which models contributed.
        """
        if usage_store is None:
            # Surface the actual reason so the UI doesn't make the
            # operator dig through docker logs to learn "your config
            # said enabled=true but sqlite couldn't open the file"
            # vs. "you literally turned it off".
            raise HTTPException(503,
                usage_disabled_reason or
                "usage accounting disabled (no reason recorded)")
        if group_by not in ("account", "worker", "litellm_user"):
            raise HTTPException(400,
                "group_by must be account | worker | litellm_user")
        if range not in ("lifecycle", "today", "7d"):
            raise HTTPException(400,
                "range must be lifecycle | today | 7d")

        now = time.time()
        if range == "today":
            # UTC midnight — keeps the cutoff deterministic regardless
            # of host timezone, and matches Anthropic's reset semantics
            # (their daily/weekly windows are UTC-anchored too).
            today = time.gmtime(now)
            since = time.mktime(time.struct_time(
                (today.tm_year, today.tm_mon, today.tm_mday,
                 0, 0, 0, 0, 0, 0)))
            # mktime returns local-epoch; convert to UTC-epoch by
            # subtracting timezone offset.
            since -= time.timezone
            until: float | None = None
        elif range == "7d":
            since = now - 7 * 86400
            until = None
        else:
            # Lifecycle: server resolves a per-bucket lower bound after
            # we know which buckets exist. Use 0.0 here for the bulk
            # query (returns everything), then filter rows below.
            since = 0.0
            until = None

        rows = usage_store.query(
            since=since, until=until, group_by=group_by)

        if range == "lifecycle":
            filtered = []
            for r in rows:
                bucket_since = manager.lifecycle_since(group_by, r["key"])
                # Re-query just this bucket's slice. Cheap — sqlite hits
                # the (group, ts) index. Avoids materialising the full
                # event log into Python just to filter.
                slice_rows = usage_store.query(
                    since=bucket_since, until=until, group_by=group_by)
                for sr in slice_rows:
                    if sr["key"] == r["key"]:
                        sr["lifecycle_since"] = bucket_since
                        filtered.append(sr)
                        break
            rows = filtered

        # Cost estimate per row requires the per-model breakdown — pricing
        # differs by model so the totals row can't just multiply once.
        out: list[dict[str, Any]] = []
        total = {"input_tokens": 0, "output_tokens": 0,
                 "cache_creation_tokens": 0, "cache_read_tokens": 0,
                 "request_count": 0, "estimated_usd": 0.0,
                 "usd_known": False}
        for r in rows:
            models = usage_store.query_by_model(
                since=since if range != "lifecycle"
                else r.get("lifecycle_since", since),
                until=until, group_by=group_by, key=r["key"])
            row_usd: float | None = 0.0
            any_known = False
            by_model_out = []
            for m in models:
                m_usd = estimate_usd(
                    m["model"],
                    input_tokens=m["input_tokens"] or 0,
                    output_tokens=m["output_tokens"] or 0,
                    cache_creation_tokens=m["cache_creation_tokens"] or 0,
                    cache_read_tokens=m["cache_read_tokens"] or 0,
                )
                if m_usd is not None:
                    row_usd = (row_usd or 0.0) + m_usd
                    any_known = True
                by_model_out.append({
                    "model": m["model"] or "(unknown)",
                    "input_tokens": m["input_tokens"] or 0,
                    "output_tokens": m["output_tokens"] or 0,
                    "cache_creation_tokens": m["cache_creation_tokens"] or 0,
                    "cache_read_tokens": m["cache_read_tokens"] or 0,
                    "request_count": m["request_count"] or 0,
                    "estimated_usd": round(m_usd, 4) if m_usd is not None else None,
                })
            out.append({
                "key": r["key"] or "(none)",
                "input_tokens": r["input_tokens"] or 0,
                "output_tokens": r["output_tokens"] or 0,
                "cache_creation_tokens": r["cache_creation_tokens"] or 0,
                "cache_read_tokens": r["cache_read_tokens"] or 0,
                "request_count": r["request_count"] or 0,
                # row_usd is null only when NOTHING under this row had
                # a known model — surface that to the UI so it can show
                # a dash instead of "$0.00" (zero would be wrong).
                "estimated_usd": round(row_usd, 4) if any_known else None,
                "lifecycle_since": r.get("lifecycle_since"),
                "by_model": by_model_out,
            })
            for k in ("input_tokens", "output_tokens",
                     "cache_creation_tokens", "cache_read_tokens",
                     "request_count"):
                total[k] += r[k] or 0
            if any_known:
                total["estimated_usd"] += row_usd or 0.0
                total["usd_known"] = True

        return {
            "ok": True,
            "range": range,
            "group_by": group_by,
            "since_epoch": None if range == "lifecycle" else since,
            "until_epoch": until,
            "now_epoch": now,
            "rows": out,
            "totals": {
                **{k: total[k] for k in ("input_tokens", "output_tokens",
                                         "cache_creation_tokens",
                                         "cache_read_tokens",
                                         "request_count")},
                "estimated_usd": (round(total["estimated_usd"], 4)
                                  if total["usd_known"] else None),
            },
        }

    return router


def _get_path(obj: Any, dotted: str) -> Any:
    """Walk a dotted attribute path on a pydantic model (e.g.
    'claude.timeouts.prewarm_seconds')."""
    cur = obj
    for part in dotted.split("."):
        cur = getattr(cur, part)
    return cur


async def _apply_oauth_refresh(
    config: Config,
    new_config: Config,
    state: SimpleNamespace,
    credentials_paths,
    changes: list[str],
    warnings: list[str],
) -> None:
    """Apply oauth_refresh.* changes. Handles enable/disable transitions
    by starting/stopping the background task; for in-place tuning,
    mutates the live refresher's fields so the running loop picks them
    up on its next iteration."""
    old_enabled = config.oauth_refresh.enabled
    new_enabled = new_config.oauth_refresh.enabled
    new_check = new_config.oauth_refresh.check_interval_seconds
    new_window = new_config.oauth_refresh.refresh_when_expires_within_seconds

    if old_enabled and not new_enabled:
        # disable: cancel running task, drop refresher
        if state.task is not None and not state.task.done():
            state.task.cancel()
            try:
                await state.task
            except asyncio.CancelledError:
                pass
        state.refresher = None
        state.task = None
        changes.append("oauth_refresh.enabled: true -> false (refresher stopped)")
        warnings.append(
            "oauth_refresh disabled — workers will self-refresh; "
            "the multi-worker refresh_token rotation race may re-emerge")
    elif not old_enabled and new_enabled:
        # enable: spawn a fresh refresher
        r = OAuthRefresher(
            credentials_paths=credentials_paths,
            check_interval_seconds=new_check,
            refresh_when_expires_within_seconds=new_window,
        )
        await r.initial_refresh()
        state.refresher = r
        state.task = asyncio.create_task(r.run(), name="oauth-refresher")
        changes.append("oauth_refresh.enabled: false -> true (refresher started)")
    elif old_enabled and new_enabled and state.refresher is not None:
        # both enabled — apply field tuning to the live refresher
        if state.refresher.check_interval != new_check:
            old = state.refresher.check_interval
            state.refresher.check_interval = new_check
            changes.append(
                f"oauth_refresh.check_interval_seconds: {old} -> {new_check}")
        if state.refresher.refresh_window != new_window:
            old = state.refresher.refresh_window
            state.refresher.refresh_window = new_window
            changes.append(
                f"oauth_refresh.refresh_when_expires_within_seconds: "
                f"{old} -> {new_window}")

    # Mutate the static config object last so subsequent reads see the
    # new values regardless of which branch above ran.
    config.oauth_refresh.enabled = new_enabled
    config.oauth_refresh.check_interval_seconds = new_check
    config.oauth_refresh.refresh_when_expires_within_seconds = new_window


async def _apply_users_diff(
    config: Config,
    new_config: Config,
    manager: SessionManager,
    changes: list[str],
) -> None:
    """Compare old vs new users pools. Stop workers whose user_id no
    longer appears in any pool; prewarm freshly-introduced user_ids.
    Pool composition changes (e.g. token X used to map to [a, b] and
    now maps to [b, c]) are handled by the union-vs-union diff: 'a' is
    removed, 'c' is added; 'b' is reused as-is."""
    old_users = {u for pool in config.users.values() for u in pool}
    new_users = {u for pool in new_config.users.values() for u in pool}
    removed = old_users - new_users
    added = new_users - old_users

    # Tokens-level diff (just for reporting)
    old_tokens = set(config.users.keys())
    new_tokens = set(new_config.users.keys())
    added_tokens = new_tokens - old_tokens
    removed_tokens = old_tokens - new_tokens
    if added_tokens or removed_tokens:
        changes.append(
            f"users tokens: +{len(added_tokens)} -{len(removed_tokens)}")

    # Swap the dict reference. Auth dep reads this per-request; old
    # request handlers that already captured config.users get the old
    # mapping (fine — they're about to finish).
    config.users = new_config.users

    # Stop workers no longer referenced
    if removed:
        stopped = []
        async with manager.lock:
            for u in removed:
                sess = manager.sessions.pop(u, None)
                if sess is not None:
                    stopped.append(sess)
        for sess in stopped:
            try:
                await sess.stop()
            except Exception:
                log.exception("error stopping removed user worker user=%s",
                              sess.user_id)
        if stopped:
            changes.append(
                f"users removed (workers stopped): {sorted(u for u in removed)}")

    # Spawn workers for newly-added users (this also prewarms)
    if added:
        prewarmed = []
        failed = []
        for u in sorted(added):
            try:
                await manager.get_or_create(u)
                prewarmed.append(u)
            except Exception:
                log.exception("prewarm failed for new user=%s", u)
                failed.append(u)
        if prewarmed:
            changes.append(f"users added (prewarmed): {prewarmed}")
        if failed:
            changes.append(
                f"users added but prewarm failed: {failed} "
                "(will cold-start on first request)")
