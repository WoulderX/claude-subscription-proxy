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

from fastapi import APIRouter, Depends, HTTPException

from ..config import Config
from ..oauth_refresh import OAuthRefresher
from ..session.manager import SessionManager

log = logging.getLogger(__name__)


# Fields that mutation-in-place handles correctly. Anything not in this
# set + not in REQUIRES_RESTART is silently ignored on reload (which
# would be a bug — keep these two sets exhaustive vs. the Config schema).
_HOT_RELOADABLE = {
    "claude.restart_interval_seconds",
    "claude.timeouts.mitm_intercept_seconds",
    "claude.timeouts.status_stall_seconds",
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
    credentials_path,
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
                note = (" (next worker spawn only)"
                        if field == "mitm_intercept_seconds" else "")
                changes.append(f"claude.timeouts.{field}: {old} -> {new}{note}")

        # --- oauth_refresh.* ---
        await _apply_oauth_refresh(
            config, new_config, refresher_state, credentials_path,
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

    @router.post("/refresh-now")
    async def refresh_now(_pool: list[str] = Depends(auth_dep)) -> dict[str, Any]:
        """Force an immediate OAuth refresh, bypassing the periodic
        loop's sleep. Useful when you suspect the on-disk token is
        stale (e.g., you just hit 401s) and don't want to wait for
        the next check_interval tick."""
        if refresher_state.refresher is None:
            raise HTTPException(503,
                "oauth_refresh is disabled (oauth_refresh.enabled=false)")
        try:
            result = await refresher_state.refresher.refresh_now()
        except Exception as e:
            log.exception("/admin/refresh-now failed")
            raise HTTPException(500, f"refresh failed: {e}")
        return {"ok": True, "result": result}

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
    credentials_path,
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
            credentials_path=credentials_path,
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
