from __future__ import annotations

from fastapi import Header, HTTPException

from .config import Config


def make_auth_dep(config: Config):
    async def auth(authorization: str | None = Header(default=None),
                   x_api_key: str | None = Header(default=None)) -> list[str]:
        """Resolve the bearer/x-api-key header to a user pool — always a
        list, even for single-user tokens (Config normalises scalars)."""
        token = None
        if authorization and authorization.lower().startswith("bearer"):
            # Slice past the "bearer" literal then strip — tolerates clients
            # that send "Bearer<tab>tok", "Bearer  tok", or just "Bearer" (no
            # token) without crashing on a missing split slot.
            token = authorization[6:].strip() or None
        if token is None and x_api_key:
            token = x_api_key.strip() or None
        if not token or token not in config.users:
            raise HTTPException(status_code=401, detail="invalid token")
        return config.users[token]
    return auth
