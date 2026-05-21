from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from .api.anthropic import build_router as build_anthropic_router
from .api.openai import build_router as build_openai_router
from .auth import make_auth_dep
from .config import Config
from .session.manager import SessionManager


def create_app(config: Config) -> FastAPI:
    manager = SessionManager(config)
    auth_dep = make_auth_dep(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await manager.start()
        try:
            yield
        finally:
            await manager.stop()

    app = FastAPI(lifespan=lifespan)
    app.include_router(build_anthropic_router(manager, auth_dep))
    app.include_router(build_openai_router(manager, auth_dep))

    @app.get("/healthz")
    async def healthz():
        return {"ok": True, "sessions": list(manager.sessions.keys())}

    @app.get("/status")
    async def status():
        """Per-worker runtime state. Distinguishes healthy in-flight
        requests (bytes flowing) from stalled ones (no bytes received
        recently, likely a leaked channel from a missed mitm intercept
        or an upstream that froze mid-stream). Unauthenticated — same
        trust model as /healthz; protect with reverse-proxy ACL if
        exposed beyond trusted hosts."""
        import time as _time
        now = _time.monotonic()
        # A request that's been alive for > this many seconds without
        # receiving a single byte from upstream is almost certainly
        # leaked rather than just slow. Anthropic typically emits its
        # first SSE event within ~3s; 30s is comfortably past any
        # plausible cold-start.
        STALL_THRESHOLD = 30.0

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
    app = create_app(config)
    uvicorn.run(app, host=config.listen_host, port=config.listen_port)


if __name__ == "__main__":
    main()
