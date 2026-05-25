from __future__ import annotations

import asyncio
import logging
import os
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import uvicorn
from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse

from .api.admin import build_router as build_admin_router
from .api.anthropic import build_router as build_anthropic_router
from .api.openai import build_router as build_openai_router
from .auth import make_admin_auth_dep, make_auth_dep
from .config import Config
from .oauth_refresh import OAuthRefresher
from .oauth_login import LoginRegistry
from .quota_probe import QuotaProbeService
from .session.manager import SessionManager
from .usage import UsageStore

log = logging.getLogger(__name__)


def _detect_claude_version(binary: str) -> str:
    """Run `<binary> --version` once at startup so the running CLI
    version can be surfaced via /healthz. The proxy is tightly coupled
    to specific CLI internals (billing-header layout, OAuth client id,
    prompt symbol); when something silently breaks after a build, the
    first thing to check is whether the pinned version actually got
    pinned. Failure is non-fatal — we'll just report "unknown"."""
    try:
        out = subprocess.run([binary, "--version"], capture_output=True,
                             text=True, timeout=10)
        return (out.stdout or out.stderr).strip() or "unknown"
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("could not detect claude CLI version: %s", e)
        return "unknown"


def create_app(config: Config, config_path: str | None = None) -> FastAPI:
    """Build the FastAPI app.

    config_path is used by /admin/reload to re-read config.yaml from
    disk; if None (e.g. config was loaded from somewhere other than a
    file), /admin/reload returns 503."""
    usage_store: UsageStore | None = None
    usage_disabled_reason: str | None = None
    if not config.usage.enabled:
        usage_disabled_reason = (
            "config.usage.enabled=false — set it to true and restart")
    else:
        try:
            usage_store = UsageStore(config.usage.db_path)
        except Exception as e:
            log.exception("usage store init failed (db_path=%r); "
                          "continuing without usage accounting",
                          config.usage.db_path)
            usage_disabled_reason = (
                f"usage store init failed for db_path={config.usage.db_path!r}: "
                f"{type(e).__name__}: {e}")
    manager = SessionManager(config, usage_store=usage_store)
    auth_dep = make_auth_dep(config)
    admin_auth_dep = make_admin_auth_dep(config, auth_dep)
    if config.admin_api_key:
        log.info("admin auth: dedicated admin_api_key in use "
                 "(tenant keys cannot access /admin/*)")
    else:
        log.warning("admin auth: NO dedicated admin_api_key set — "
                    "any tenant key can call /admin/*. Set "
                    "`admin_api_key:` in config.yaml to harden.")
    claude_version = _detect_claude_version(config.claude.binary)
    log.info("claude code CLI version: %s", claude_version)

    # Dashboard-driven OAuth login flows. Only useful in multi-account
    # mode (legacy single-account mode has no /admin/accounts/new
    # surface area). Sweeper is started in lifespan below.
    login_registry: LoginRegistry | None = None
    if config.accounts:
        login_registry = LoginRegistry()

    # Per-account quota tracker. Only meaningful in multi-account mode —
    # each account has its own .credentials.json and thus its own
    # subscription quota. Legacy single-account deployments get None
    # here and /admin/quotas returns 503.
    quota_probe: QuotaProbeService | None = None
    if config.accounts:
        # Cooldown file lives alongside the usage sqlite db (already
        # RW-mounted from the host). Falls back to None — no
        # persistence — when usage tracking is disabled or there's
        # no writable mount: in that case rebuilds keep retriggering
        # 429s but the in-memory cooldown gate still works during the
        # container's lifetime.
        cooldown_path = None
        if config.usage.db_path:
            from pathlib import Path
            cooldown_path = Path(config.usage.db_path).parent / "quota_cooldown.yaml"
        quota_probe = QuotaProbeService(
            manager=manager,
            accounts=list(config.accounts.keys()),
            tick_seconds=300.0,
            cooldown_path=cooldown_path,
        )
        # Manager fans worker quota_usage events into the service keyed
        # by account name (worker → account resolution happens inside
        # the manager so the session doesn't have to know).
        manager.quota_record_cb = quota_probe.record
        manager.quota_429_cb = quota_probe.record_429

    # In multi-account mode this returns one .credentials.json per
    # account (under /data/shared-auth/<acc>/.credentials.json); in
    # legacy mode it's the single operator path. The refresher iterates
    # them each tick and refreshes any that's within its expiry window.
    credentials_paths = config.credentials_paths()

    # Refresher + its task are owned by lifespan but admin/reload also
    # needs to swap them (when oauth_refresh.enabled toggles). Holding
    # them on a SimpleNamespace gives both code paths a stable
    # reference; this is single-threaded asyncio so plain attribute
    # access is safe.
    refresher_state = SimpleNamespace(refresher=None, task=None)

    if config.oauth_refresh.enabled:
        refresher_state.refresher = OAuthRefresher(
            credentials_paths=credentials_paths,
            check_interval_seconds=config.oauth_refresh.check_interval_seconds,
            refresh_when_expires_within_seconds=
                config.oauth_refresh.refresh_when_expires_within_seconds,
        )
    else:
        log.warning("oauth_refresh.enabled=false — workers will self-refresh; "
                    "multi-worker refresh_token rotation race may re-emerge")

    quota_state = SimpleNamespace(task=None)
    login_state = SimpleNamespace(task=None)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Refresh once SYNCHRONOUSLY before any worker spawns, so the
        # first worker's HOME read picks up a fresh token if the
        # deployment had been sitting on a stale one. Then kick off
        # the background loop to keep it fresh.
        if refresher_state.refresher is not None:
            await refresher_state.refresher.initial_refresh()
            refresher_state.task = asyncio.create_task(
                refresher_state.refresher.run(), name="oauth-refresher")
        await manager.start()
        # Quota probe runs AFTER manager.start because prewarm populates
        # the sessions dict (probe needs an idle worker per account).
        # Initial probe is fire-and-forget — it logs failures but never
        # blocks startup, and the periodic tick will retry every 5 min.
        if quota_probe is not None:
            asyncio.create_task(quota_probe.initial_probe(),
                                name="quota-initial-probe")
            quota_state.task = asyncio.create_task(
                quota_probe.run(), name="quota-probe")
        if login_registry is not None:
            # Sweeper drops abandoned login flows (10min lifetime) so
            # orphaned PTY procs + tempdirs don't pile up.
            login_state.task = asyncio.create_task(
                login_registry.run_sweeper(), name="oauth-login-sweeper")
        try:
            yield
        finally:
            if login_state.task is not None:
                login_state.task.cancel()
                try:
                    await login_state.task
                except asyncio.CancelledError:
                    pass
            if quota_state.task is not None:
                quota_state.task.cancel()
                try:
                    await quota_state.task
                except asyncio.CancelledError:
                    pass
            await manager.stop()
            if refresher_state.task is not None:
                refresher_state.task.cancel()
                try:
                    await refresher_state.task
                except asyncio.CancelledError:
                    pass

    app = FastAPI(lifespan=lifespan)
    app.include_router(build_anthropic_router(manager, auth_dep))
    app.include_router(build_openai_router(manager, auth_dep))
    app.include_router(build_admin_router(
        manager=manager, config=config, config_path=config_path,
        auth_dep=admin_auth_dep, refresher_state=refresher_state,
        credentials_paths=credentials_paths,
        usage_store=usage_store,
        usage_disabled_reason=usage_disabled_reason,
        quota_probe=quota_probe,
        login_registry=login_registry,
    ))

    @app.get("/healthz")
    async def healthz():
        # Liveness probe — must stay unauthenticated for k8s / docker
        # healthcheck access. Keep the response surface minimal so it
        # doesn't reveal worker topology (account names, replica counts)
        # to unauthenticated callers on the listen interface; richer
        # state lives behind admin_auth_dep on /status.
        return {"ok": True, "claude_version": claude_version}

    # Static admin/monitoring page. Unauthenticated by design — the HTML
    # itself doesn't expose any state, only loads if the user fills in
    # an API key (stored in their browser's localStorage), and from then
    # on the page uses that key to call /status and /admin/* (both of
    # which ARE auth-protected). Serving the HTML behind auth_dep would
    # create a chicken-and-egg: the browser can't supply the Bearer
    # header on its initial document GET.
    _admin_html = Path(__file__).parent / "static" / "admin.html"

    @app.get("/ui")
    async def admin_ui():
        return FileResponse(_admin_html, media_type="text/html")

    @app.get("/status")
    async def status(_pool: list[str] = Depends(admin_auth_dep)):
        """Per-worker runtime state. Distinguishes healthy in-flight
        requests (bytes flowing) from stalled ones (no bytes received
        recently, likely a leaked channel from a missed mitm intercept
        or an upstream that froze mid-stream).

        Auth: admin_api_key when set in config, else falls back to any
        configured tenant key (legacy behavior). /status leaks request
        metadata (model, n_messages, last_user_preview of in-flight
        prompts) across ALL tenants, so once a deployment shares its
        tenant key with downstream clients (LiteLLM, OpenAI SDKs, …)
        the admin_api_key split is the only thing keeping that data
        scoped to the operator. /healthz stays open for liveness."""
        import time as _time
        now = _time.monotonic()
        # A request that's been alive for > this many seconds without
        # receiving a single byte from upstream is almost certainly
        # leaked rather than just slow. Anthropic typically emits its
        # first SSE event within ~3s; the default 30s is comfortably
        # past any plausible cold-start, but is configurable via
        # claude.timeouts.status_stall_seconds for workloads with
        # genuinely long initial latency.
        STALL_THRESHOLD = config.claude.timeouts.status_stall_seconds

        # Snapshot current per-account issue state once so the per-worker
        # loop can flag each row without re-querying the manager each
        # iteration (also expires stale entries lazily via the manager's
        # accessor). Value is the kind ("rate_limit" / "degraded"); the
        # UI uses it to pick the right badge colour + label per worker
        # ("已限流" vs "不可用").
        account_issue_kind: dict[str, str] = {}
        if config.accounts:
            for _name in config.accounts.keys():
                _state = manager.account_rate_limit(_name)
                if _state is not None:
                    account_issue_kind[_name] = _state.kind

        workers = []
        # Snapshot the dict so a concurrent restart can't mutate under us.
        for user_id, sess in list(manager.sessions.items()):
            pid = sess.proc.pid if sess.proc else None
            alive = sess.proc is not None and sess.proc.returncode is None
            rc = sess.proc.returncode if sess.proc else None

            in_flight_detail = []
            for req_id, ch in list(sess._channels.items()):
                age = now - ch.created_at
                stalled = now - ch.last_chunk_at
                in_flight_detail.append({
                    "req_id": req_id,
                    "age_seconds": round(age, 1),
                    "stalled_seconds": round(stalled, 1),
                    # No bytes received yet on this channel: stalled and
                    # age are equal (last_chunk_at = created_at). Useful
                    # to distinguish "never got a byte" vs "got bytes,
                    # then went silent" — the former is the leaked-
                    # channel pattern, the latter is upstream slowing
                    # down mid-stream.
                    "ever_received_bytes": ch.last_chunk_at != ch.created_at,
                    "bytes_received": ch.bytes_received,
                    # Tells you WHAT the worker is processing right now:
                    # model, n_messages, max_tokens, first 80 chars of
                    # the last user message. Helps spot "all 20 workers
                    # are stuck on the same prompt" patterns at a glance.
                    "body": ch.body_summary,
                })

            # Stuck heuristic: any in-flight request older than
            # STALL_THRESHOLD that has either received no bytes ever, or
            # gone silent for STALL_THRESHOLD. Healthy long-running calls
            # still emit a chunk every couple of seconds, so a 30 s gap
            # is decisive.
            stuck = any(d["stalled_seconds"] > STALL_THRESHOLD
                        for d in in_flight_detail)

            # Account this worker belongs to (multi-account mode). null
            # in legacy single-account mode; the UI uses this to drive
            # an account column for operator triage ("which account is
            # being rate-limited?").
            acc = config.account_for_user(user_id)
            account_name = None
            if acc is not None:
                for n, a in config.accounts.items():
                    if a is acc:
                        account_name = n
                        break

            _wkind = account_issue_kind.get(account_name)
            workers.append({
                "user_id": user_id,
                "account": account_name,
                # True iff this worker's account currently has ANY
                # routing-block (rate_limit OR degraded). UI prefers
                # this over idle/working/stuck — a worker on a blocked
                # account shouldn't show as "活跃" regardless of any
                # residual in_flight count.
                "rate_limited": _wkind is not None,
                # Specific kind, so the UI can label the badge
                # differently: "已限流" for rate_limit, "不可用" for
                # degraded. None when the account is healthy.
                "issue_kind": _wkind,
                "mitm_port": sess.mitm_port,
                "pid": pid,
                "alive": alive,
                "exit_code": rc,                          # null if alive
                "in_flight": len(sess._channels),         # active right now
                "stuck": stuck,                           # heuristic
                "in_flight_detail": in_flight_detail,     # per-request timing
                "lock_held": sess.lock.locked(),
                "total_requests": sess._next_req_id,      # since last restart
                "age_seconds": round(sess.age_seconds(), 1),
                "idle_seconds": round(sess.idle_seconds(), 1),
            })
        workers.sort(key=lambda w: w["user_id"])

        # Account-level rate-limit state. Lazy-expire on read so the UI
        # never shows a stale "limited" badge after the window closes.
        # Emit one entry per configured account (multi-account mode
        # only) — UI uses this to render a per-account badge + a stat
        # card counting how many accounts are currently limited.
        import time as _time
        wall_now = _time.time()
        accounts_out = []
        if config.accounts:
            for name in sorted(config.accounts.keys()):
                state = manager.account_rate_limit(name)
                accounts_out.append({
                    "name": name,
                    # `kind` distinguishes positive rate-limit signals
                    # ("rate_limit") from unknown-cause failures
                    # ("degraded"). Both block routing; UI shows them
                    # differently so the operator's response can
                    # match ("wait for reset" vs. "investigate").
                    "issue_kind": state.kind if state else None,
                    "rate_limited": (
                        state.kind == "rate_limit" if state else False),
                    "degraded": (
                        state.kind == "degraded" if state else False),
                    "rate_limit_reason": state.reason if state else None,
                    "rate_limited_since": (
                        round(state.set_at) if state else None),
                    "rate_limited_until": (
                        round(state.until) if state else None),
                    "rate_limited_seconds_remaining": (
                        round(state.until - wall_now) if state else 0),
                    # Worker count belonging to this account — handy in
                    # the UI to surface "5/5 workers are unusable".
                    "worker_count": sum(
                        1 for w in workers if w.get("account") == name),
                })

        rate_limited_account_count = sum(
            1 for a in accounts_out if a["rate_limited"])
        degraded_account_count = sum(
            1 for a in accounts_out if a["degraded"])

        return {
            "ok": True,
            "claude_version": claude_version,
            "worker_count": len(workers),
            "alive_count": sum(1 for w in workers if w["alive"]),
            "busy_count": sum(1 for w in workers if w["in_flight"] > 0),
            "stuck_count": sum(1 for w in workers if w["stuck"]),
            "rate_limited_account_count": rate_limited_account_count,
            "degraded_account_count": degraded_account_count,
            "workers": workers,
            "accounts": accounts_out,
        }

    return app


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config_path = os.environ.get("CONFIG", "config.yaml")
    config = Config.load(config_path)
    app = create_app(config, config_path=config_path)
    # Suppress per-request access lines from uvicorn — with the
    # dashboard auto-polling /status + /admin/usage + /admin/quotas
    # they add ~4 lines/minute of noise that obscures real events.
    # Errors and lifespan messages still come through.
    uvicorn.run(app, host=config.listen_host, port=config.listen_port,
                access_log=False)


if __name__ == "__main__":
    main()
