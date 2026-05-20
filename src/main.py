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
