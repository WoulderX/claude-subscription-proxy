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
from pathlib import Path
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
    "accounts",
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
    quota_probe=None,
    login_registry=None,
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

        # --- claude.session_affinity{,_ttl_seconds} + rate_limit backoff ---
        # All read live in SessionManager (pick() / mark_account_rate_limited),
        # so mutating in place takes effect on the very next request — lets
        # an operator A/B the routing + backoff strategy without a restart.
        for field in ("session_affinity", "session_affinity_ttl_seconds",
                      "rate_limit_base_cooldown_seconds",
                      "rate_limit_max_cooldown_seconds"):
            old = getattr(config.claude, field)
            new = getattr(new_config.claude, field)
            if old != new:
                setattr(config.claude, field, new)
                changes.append(f"claude.{field}: {old} -> {new}")

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

        # --- accounts (add / remove / worker-count diff) ---
        # Must run BEFORE users diff so the user pool entries that
        # reference the new account's user_ids land on already-spawned
        # sessions (get_or_create is happy to grab an existing one).
        await _apply_accounts_diff(config, new_config, manager, changes)

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

    @router.get("/quotas")
    async def get_quotas(
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Per-account subscription quota (5h / 7d / 7d-per-model
        utilization). Snapshots are refreshed in the background every
        `tick_seconds` (default 300s) by sending /usage to one idle
        worker per account; mitm captures the resulting
        /api/oauth/usage response.

        Returns 503 if QuotaProbeService is disabled (legacy single-
        account mode or no `accounts:` block in config).

        Response shape:
          {
            "tick_seconds": 300,
            "accounts": {
              "claude-1": {
                "snapshot": {five_hour, seven_day, ...} | null,
                "last_error": {error, attempted_at_unix} | null,
                "age_seconds": <int> | null,
                "seconds_until_next_tick": <int>
              },
              ...
            }
          }"""
        if quota_probe is None:
            raise HTTPException(503,
                "quota probe service not enabled "
                "(requires multi-account config with `accounts:` block)")
        return {"ok": True, **quota_probe.state_dict()}

    @router.post("/accounts/new")
    async def begin_account_login(
        payload: dict[str, Any] = Body(...),
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Start a `claude auth login --claudeai` flow for a new
        account. Returns the OAuth authorize URL — the dashboard opens
        it in a browser, the operator authorizes, and the resulting
        callback code gets pasted back to POST .../finish.

        Body: {"name": "claude-4", "workers": 5}  (workers optional, default 5)

        Errors:
          - 400 if name is invalid or already in use
          - 503 if login_registry is not configured (legacy mode)
          - 504 if the CLI doesn't print the URL in time"""
        if login_registry is None:
            raise HTTPException(503,
                "OAuth login flow unavailable — login_registry not "
                "wired (legacy single-account mode?)")
        name = payload.get("name")
        workers = payload.get("workers") or 5
        if not isinstance(name, str) or not name or "/" in name:
            raise HTTPException(400,
                "name must be a non-empty string with no '/' (e.g. 'claude-4')")
        if name in config.accounts:
            raise HTTPException(400,
                f"account {name!r} already exists in config; pick another name")
        if not isinstance(workers, int) or workers < 1 or workers > 20:
            raise HTTPException(400,
                "workers must be an int in [1, 20]")
        # Reserve the dest directory inside the shared-auth tree so
        # finish() lands at a predictable path. The parent bind mount
        # makes this visible on the host.
        dest_dir = Path("/data/shared-auth") / name
        if dest_dir.exists() and any(dest_dir.iterdir()):
            raise HTTPException(400,
                f"{dest_dir} already has contents — refuse to clobber. "
                f"Remove the directory manually if you want to re-create "
                f"this account.")
        try:
            url = await login_registry.begin(
                name, dest_dir, claude_binary=config.claude.binary)
        except Exception:
            # Detail is server-side only; PTY output may include
            # operator-pasted secrets and must not propagate via 500
            # body. See oauth_login.py.
            log.exception("OAuth login begin failed for account=%s", name)
            raise HTTPException(500,
                "login begin failed; check container logs for details")
        return {
            "ok": True,
            "account": name,
            "workers": workers,
            "authorize_url": url,
        }

    @router.post("/accounts/new/{name}/finish")
    async def finish_account_login(
        name: str,
        payload: dict[str, Any] = Body(...),
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Submit the OAuth callback code, complete the CLI's token
        exchange, install credentials at /data/shared-auth/{name}/,
        then add the account to config + spawn workers.

        Body: {"code": "...", "workers": 5}  (workers optional, default 5)"""
        if login_registry is None:
            raise HTTPException(503, "OAuth login flow unavailable")
        code = payload.get("code")
        workers = payload.get("workers") or 5
        if not isinstance(code, str) or not code.strip():
            raise HTTPException(400, "code must be a non-empty string")
        if not isinstance(workers, int) or workers < 1 or workers > 20:
            raise HTTPException(400, "workers must be an int in [1, 20]")
        try:
            installed = await login_registry.finish(name, code)
        except KeyError:
            raise HTTPException(404,
                f"no in-progress login flow for account {name!r}; "
                "did POST /admin/accounts/new run first?")
        except Exception:
            # Detail is server-side only; finish() PTY tail contains the
            # operator-typed auth code which must not flow into HTTP
            # response bodies. See oauth_login.py.
            log.exception("OAuth login finish failed for account=%s", name)
            raise HTTPException(500,
                "login finish failed; check container logs for details")

        # Wire the new account into config + spawn workers. The
        # AccountConfig type is imported at module-level; reuse it.
        from ..config import AccountConfig
        dest_dir = str(Path("/data/shared-auth") / name)
        config.accounts[name] = AccountConfig(dir=dest_dir, workers=workers)
        # Extend the auto-populated api_key pool if it exists (mirrors
        # Config._wire_accounts behavior on cold start). If the operator
        # is running with explicit per-account `users:` pools, they have
        # to wire the new account themselves — we don't know which key
        # should map to it.
        new_user_ids = [f"{name}-{i}" for i in range(workers)]
        if config.api_key and config.api_key in config.users:
            existing = list(config.users[config.api_key])
            for uid in new_user_ids:
                if uid not in existing:
                    existing.append(uid)
            config.users[config.api_key] = existing
        # Persist to the runtime overlay so the account survives
        # container restarts without manual config.yaml edits.
        try:
            config.write_runtime_accounts()
        except Exception:
            log.exception("failed to persist runtime accounts overlay")
        # Spawn workers. background_rest=True keeps this HTTP request
        # snappy (~10s for the first-worker validation, vs ~50s for a
        # full 5-worker chain) — the remaining workers spawn in a
        # background task, and the dashboard's /status polling reflects
        # them as they come up.
        try:
            spawned = await manager.spawn_account(name, background_rest=True)
        except Exception as e:
            log.exception("spawn_account failed after successful login")
            # Don't roll back config — credentials are written, the
            # account exists. Operator can manually trigger a restart
            # to retry the spawn.
            raise HTTPException(500,
                f"login succeeded but spawn_account failed: {e}; "
                f"credentials are installed at /data/shared-auth/{name}/, "
                f"restart the container to retry")
        # Register with the quota probe so the dashboard's 订阅配额
        # panel picks up the new account on its next /admin/quotas
        # poll instead of waiting for a container restart. probe_now
        # kicks one immediately so the panel fills without the 300s
        # tick latency. Both are no-ops in legacy single-account mode.
        if quota_probe is not None:
            quota_probe.register_account(name)
            quota_probe.probe_now(name)
        return {
            "ok": True,
            "account": name,
            "workers_spawned": len(spawned),
            "workers_total": workers,
            "workers_pending": max(0, workers - len(spawned)),
            "credentials_path": installed.get("credentials_path"),
        }

    @router.post("/accounts/new/{name}/abort")
    async def abort_account_login(
        name: str,
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Cancel an in-progress flow (e.g. operator changed their
        mind). Idempotent — returns ok=true even if nothing to abort."""
        if login_registry is None:
            raise HTTPException(503, "OAuth login flow unavailable")
        aborted = await login_registry.abort(name)
        return {"ok": True, "aborted": aborted}

    @router.delete("/accounts/{name}")
    async def delete_account(
        name: str,
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Stop all workers for an account and remove it from config.
        The on-disk credentials at /data/shared-auth/{name}/ are
        DELETED (account is gone for good).

        Use /admin/accounts/{name}/clear-rate-limit if you just want
        to clear a rate-limit mark without removing the account."""
        if name not in config.accounts:
            raise HTTPException(404,
                f"account {name!r} not in config.accounts; "
                f"available: {sorted(config.accounts.keys())}")
        try:
            stopped = await manager.stop_account(name)
        except Exception as e:
            log.exception("stop_account failed for %s", name)
            raise HTTPException(500, f"stop_account failed: {e}")
        # Drop quota probe registration so the deleted account no longer
        # shows on /admin/quotas + stops eating periodic ticks. Mirrors
        # the register_account call in /admin/accounts/new/.../finish.
        if quota_probe is not None:
            quota_probe.unregister_account(name)
        # Drop from config + prune user pools.
        config.accounts.pop(name, None)
        prefix = f"{name}-"
        for token, pool in list(config.users.items()):
            kept = [u for u in pool if not u.startswith(prefix)]
            if not kept:
                config.users.pop(token, None)
            else:
                config.users[token] = kept
        # Persist runtime overlay (or remove file if no dynamic accounts left)
        try:
            config.write_runtime_accounts()
        except Exception:
            log.exception("failed to persist runtime accounts overlay")
        # Wipe credentials on disk
        import shutil
        dest_dir = Path("/data/shared-auth") / name
        try:
            if dest_dir.is_dir():
                shutil.rmtree(dest_dir)
        except Exception:
            log.exception("failed to remove %s", dest_dir)
        return {
            "ok": True,
            "account": name,
            "workers_stopped": stopped,
            "dir_removed": str(dest_dir),
        }

    @router.get("/usage")
    async def get_usage(
        range: str = "today",
        group_by: str = "account",
        since: float | None = None,
        until: float | None = None,
        _pool: list[str] = Depends(auth_dep),
    ) -> dict[str, Any]:
        """Token-usage aggregates.

        Query params:
          range:    lifecycle | today | yesterday | 7d | 30d
                    | this_month | custom
                    - "lifecycle" filters by ts >= the earliest live
                      worker's started_at (per group bucket); since/
                      until query params are ignored.
                    - "custom" requires `since` (and optionally `until`)
                      as Unix epoch seconds, computed by the caller.
                      The dashboard uses this for every preset so
                      ranges align with the operator's local timezone
                      (UTC presets below are kept for compat).
                    - "today" / "yesterday" / "this_month" — UTC
                      calendar boundaries. Kept for back-compat with
                      direct API callers; the dashboard now sends
                      `range=custom` + locally-computed since/until.
                    - "7d" / "30d" — rolling now-N*86400.
          group_by: account | worker | litellm_user
          since:    Unix epoch seconds; lower bound (inclusive). Honored
                    when `range=custom` (or any non-lifecycle range
                    when supplied — overrides the server-computed value).
          until:    Unix epoch seconds; upper bound (exclusive). Optional
                    even with range=custom (defaults to now).

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
        if group_by not in ("account", "worker", "litellm_user", "pool"):
            raise HTTPException(400,
                "group_by must be account | worker | litellm_user | pool")
        # "pool" groups by front-door key (config.users). Pools are
        # account-disjoint, so we run the normal per-account aggregation
        # (query + by_model + usd + lifecycle) and fold accounts into their
        # key at the end. effective_group drives the sqlite query.
        effective_group = "account" if group_by == "pool" else group_by
        valid_ranges = ("lifecycle", "today", "yesterday", "7d", "30d",
                        "this_month", "custom")
        if range not in valid_ranges:
            raise HTTPException(400,
                f"range must be one of: {' | '.join(valid_ranges)}")

        # ── helpers ─────────────────────────────────────────────
        def _utc_midnight(epoch: float) -> float:
            """UTC midnight at or before `epoch`, in Unix epoch."""
            t = time.gmtime(epoch)
            mt = time.mktime(time.struct_time(
                (t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, 0)))
            # mktime returns local-epoch; subtract tz offset for UTC.
            return mt - time.timezone

        # Reject negative bounds and out-of-order ranges early —
        # otherwise sqlite would happily run a degenerate query and
        # return zero rows, which looks like "no data" to operators.
        if since is not None and since < 0:
            raise HTTPException(400, "since must be >= 0")
        if until is not None and until < 0:
            raise HTTPException(400, "until must be >= 0")
        if since is not None and until is not None and until < since:
            raise HTTPException(400, "until must be >= since")

        now = time.time()
        if range == "custom":
            if since is None:
                raise HTTPException(400,
                    "range=custom requires `since` query param")
            # until=None → "up to now", handled naturally by sqlite
        elif range == "lifecycle":
            # Server resolves a per-bucket lower bound after we know
            # which buckets exist. Use 0.0 here for the bulk query
            # (returns everything), then filter rows below.
            since = 0.0
            until = None
        elif since is None and until is None:
            # Named range, no client override → compute UTC bounds
            # server-side (legacy behavior — used by direct API hits).
            if range == "today":
                since = _utc_midnight(now)
                until = None
            elif range == "yesterday":
                today_mid = _utc_midnight(now)
                since = today_mid - 86400
                until = today_mid
            elif range == "this_month":
                t = time.gmtime(now)
                mt = time.mktime(time.struct_time(
                    (t.tm_year, t.tm_mon, 1, 0, 0, 0, 0, 0, 0)))
                since = mt - time.timezone
                until = None
            elif range == "7d":
                since = now - 7 * 86400
                until = None
            elif range == "30d":
                since = now - 30 * 86400
                until = None
        # else: client passed since/until for a named range — honor them.

        rows = usage_store.query(
            since=since, until=until, group_by=effective_group)

        if range == "lifecycle":
            filtered = []
            for r in rows:
                bucket_since = manager.lifecycle_since(effective_group, r["key"])
                # Re-query just this bucket's slice. Cheap — sqlite hits
                # the (group, ts) index. Avoids materialising the full
                # event log into Python just to filter.
                slice_rows = usage_store.query(
                    since=bucket_since, until=until, group_by=effective_group)
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
                until=until, group_by=effective_group, key=r["key"])
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

        # Fold per-account rows into front-door-key pools (group_by=pool).
        # Pools are account-disjoint, so summing each account into its key
        # is exact; the grand `total` is unchanged (sum over accounts ==
        # sum over pools). Unmapped accounts bucket under "(未分配)".
        if group_by == "pool":
            def _mask_key(k: str) -> str:
                return (k[:16] + "…") if len(k) > 18 else k

            def _acct_name(uid: str) -> str:
                sep = uid.rfind("-")
                return uid[:sep] if sep > 0 and uid[sep + 1:].isdigit() else uid

            acct_to_label: dict[str, str] = {}
            for token, workers in config.users.items():
                label = _mask_key(token)
                for uid in workers:
                    acct_to_label.setdefault(_acct_name(uid), label)

            pooled: dict[str, dict[str, Any]] = {}
            for r in out:
                label = acct_to_label.get(r["key"], "(未分配)")
                p = pooled.get(label)
                if p is None:
                    p = {"key": label, "input_tokens": 0, "output_tokens": 0,
                         "cache_creation_tokens": 0, "cache_read_tokens": 0,
                         "request_count": 0, "estimated_usd": None,
                         "lifecycle_since": r.get("lifecycle_since"),
                         "_by_model": {}}
                    pooled[label] = p
                for k in ("input_tokens", "output_tokens",
                          "cache_creation_tokens", "cache_read_tokens",
                          "request_count"):
                    p[k] += r[k]
                if r["estimated_usd"] is not None:
                    p["estimated_usd"] = (p["estimated_usd"] or 0.0) + r["estimated_usd"]
                for m in r["by_model"]:
                    bm = p["_by_model"].get(m["model"])
                    if bm is None:
                        bm = {"model": m["model"], "input_tokens": 0,
                              "output_tokens": 0, "cache_creation_tokens": 0,
                              "cache_read_tokens": 0, "request_count": 0,
                              "estimated_usd": None}
                        p["_by_model"][m["model"]] = bm
                    for k in ("input_tokens", "output_tokens",
                              "cache_creation_tokens", "cache_read_tokens",
                              "request_count"):
                        bm[k] += m[k]
                    if m["estimated_usd"] is not None:
                        bm["estimated_usd"] = (bm["estimated_usd"] or 0.0) + m["estimated_usd"]
            out = []
            for p in pooled.values():
                bms = list(p.pop("_by_model").values())
                for bm in bms:
                    if bm["estimated_usd"] is not None:
                        bm["estimated_usd"] = round(bm["estimated_usd"], 4)
                if p["estimated_usd"] is not None:
                    p["estimated_usd"] = round(p["estimated_usd"], 4)
                p["by_model"] = bms
                out.append(p)

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


async def _apply_accounts_diff(
    config: Config,
    new_config: Config,
    manager: SessionManager,
    changes: list[str],
) -> None:
    """Compare old vs new accounts dict and reconcile via SessionManager.

    Three kinds of change:
      - added (name in new but not old)    → spawn workers + prewarm
      - removed (in old but not new)       → drain + stop workers
      - workers-count changed for same name → log warning, NOT applied
        (changing N would require selectively spawning or stopping
        workers within an account; we leave that for a future commit
        and just refuse — operator can remove + re-add the account if
        they really need to resize it).

    Workers are spawned BEFORE config.accounts is mutated so that the
    SessionManager's get_or_create / pick path sees the new account
    consistently after this returns. Same for removal: stop_account is
    awaited before we drop the dict entry."""
    old_names = set(config.accounts.keys())
    new_names = set(new_config.accounts.keys())
    added_names = new_names - old_names
    removed_names = old_names - new_names

    # Worker-count change without name change — refuse for now.
    for name in (old_names & new_names):
        old_n = config.accounts[name].workers
        new_n = new_config.accounts[name].workers
        if old_n != new_n:
            changes.append(
                f"accounts.{name}.workers: {old_n} -> {new_n} "
                "(ignored — resize requires remove + re-add for now)")

    # Add: install in self.config.accounts first, then spawn (manager
    # reads config.accounts to resolve which account a user_id belongs
    # to during spawn).
    for name in sorted(added_names):
        config.accounts[name] = new_config.accounts[name]
        try:
            spawned = await manager.spawn_account(name)
            changes.append(
                f"accounts added: {name} ({len(spawned)} workers spawned)")
        except Exception as e:
            log.exception("spawn_account failed for %s", name)
            # Roll back the config.accounts mutation so an unspawned
            # account doesn't show up in /status as a phantom.
            config.accounts.pop(name, None)
            changes.append(f"accounts add FAILED: {name}: {e}")

    # Remove: stop workers first, then drop the config entry. Workers
    # whose account just got pulled have nothing to symlink ~/.claude
    # to — keeping them alive would be a no-op.
    for name in sorted(removed_names):
        try:
            stopped = await manager.stop_account(name)
            config.accounts.pop(name, None)
            changes.append(
                f"accounts removed: {name} ({stopped} workers stopped)")
        except Exception as e:
            log.exception("stop_account failed for %s", name)
            changes.append(f"accounts remove FAILED: {name}: {e}")


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
