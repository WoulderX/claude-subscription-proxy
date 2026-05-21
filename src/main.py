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
        """Per-worker runtime state. Useful to see which workers are
        idle / busy, how many requests they've handled, whether any
        died unnoticed. Unauthenticated — same trust model as /healthz;
        protect with reverse-proxy ACL if exposed beyond trusted hosts."""
        workers = []
        # Snapshot the dict so a concurrent restart can't mutate under us.
        for user_id, sess in list(manager.sessions.items()):
            pid = sess.proc.pid if sess.proc else None
            alive = sess.proc is not None and sess.proc.returncode is None
            rc = sess.proc.returncode if sess.proc else None
            workers.append({
                "user_id": user_id,
                "mitm_port": sess.mitm_port,
                "pid": pid,
                "alive": alive,
                "exit_code": rc,                          # null if alive
                "in_flight": len(sess._channels),         # active requests right now
                "lock_held": sess.lock.locked(),          # submission lock state
                "total_requests": sess._next_req_id,      # since last restart (resets every 12h)
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
