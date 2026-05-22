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
from .auth import make_auth_dep
from .config import Config
from .oauth_refresh import OAuthRefresher
from .session.manager import SessionManager

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
    manager = SessionManager(config)
    auth_dep = make_auth_dep(config)
    claude_version = _detect_claude_version(config.claude.binary)
    log.info("claude code CLI version: %s", claude_version)

    credentials_path = Path(os.path.expanduser("~")) / ".claude" / ".credentials.json"

    # Refresher + its task are owned by lifespan but admin/reload also
    # needs to swap them (when oauth_refresh.enabled toggles). Holding
    # them on a SimpleNamespace gives both code paths a stable
    # reference; this is single-threaded asyncio so plain attribute
    # access is safe.
    refresher_state = SimpleNamespace(refresher=None, task=None)

    if config.oauth_refresh.enabled:
        refresher_state.refresher = OAuthRefresher(
            credentials_path=credentials_path,
            check_interval_seconds=config.oauth_refresh.check_interval_seconds,
            refresh_when_expires_within_seconds=
                config.oauth_refresh.refresh_when_expires_within_seconds,
        )
    else:
        log.warning("oauth_refresh.enabled=false — workers will self-refresh; "
                    "multi-worker refresh_token rotation race may re-emerge")

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
        try:
            yield
        finally:
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
        auth_dep=auth_dep, refresher_state=refresher_state,
        credentials_path=credentials_path,
    ))

    @app.get("/healthz")
    async def healthz():
        return {
            "ok": True,
            "sessions": list(manager.sessions.keys()),
            "claude_version": claude_version,
        }

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
    async def status(_pool: list[str] = Depends(auth_dep)):
        """Per-worker runtime state. Distinguishes healthy in-flight
        requests (bytes flowing) from stalled ones (no bytes received
        recently, likely a leaked channel from a missed mitm intercept
        or an upstream that froze mid-stream).

        Auth: any configured API key (Bearer or x-api-key). /status
        leaks request metadata (model, n_messages, last_user_preview
        of in-flight prompts), so it must not be world-readable. We
        reuse the tenant key space rather than introduce a separate
        admin token because every legitimate caller already has one;
        /healthz stays open for container liveness probes."""
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

            workers.append({
                "user_id": user_id,
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

        # Pool topology — show which user_ids share a token, without
        # leaking the tokens themselves.
        pools = [list(members) for members in config.users.values()
                 if len(members) > 1]

        return {
            "ok": True,
            "claude_version": claude_version,
            "worker_count": len(workers),
            "alive_count": sum(1 for w in workers if w["alive"]),
            "busy_count": sum(1 for w in workers if w["in_flight"] > 0),
            "stuck_count": sum(1 for w in workers if w["stuck"]),
            "workers": workers,
            "pools": pools,
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
    uvicorn.run(app, host=config.listen_host, port=config.listen_port)


if __name__ == "__main__":
    main()
