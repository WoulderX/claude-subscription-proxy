from __future__ import annotations

import hmac

from fastapi import Header, HTTPException

from .config import Config


def _extract_token(authorization: str | None, x_api_key: str | None) -> str | None:
    """Pull a bearer token out of Authorization or X-Api-Key, in that
    order. Tolerates loose `Bearer<tab>tok` / `Bearer  tok` / bare
    `Bearer` (no token) inputs the same way clients in the wild send."""
    if authorization and authorization.lower().startswith("bearer"):
        tok = authorization[6:].strip() or None
        if tok is not None:
            return tok
    if x_api_key:
        return x_api_key.strip() or None
    return None


def make_auth_dep(config: Config):
    async def auth(authorization: str | None = Header(default=None),
                   x_api_key: str | None = Header(default=None)) -> list[str]:
        """Tenant auth: resolve the bearer/x-api-key header to a user
        pool — always a list, even for single-user tokens (Config
        normalises scalars)."""
        token = _extract_token(authorization, x_api_key)
        if not token or token not in config.users:
            raise HTTPException(status_code=401, detail="invalid token")
        return config.users[token]
    return auth


def make_admin_auth_dep(config: Config, tenant_auth_dep):
    """Admin auth: accept config.admin_api_key when set, otherwise fall
    back to the tenant key (legacy behavior).

    Splitting admin from tenant matters when the tenant key is shared
    with downstream callers (LiteLLM users, OpenAI-clients, etc.). A
    leaked tenant key shouldn't allow /admin/accounts/{name} DELETE or
    /admin/refresh-now.

    Uses hmac.compare_digest to avoid timing-side-channel leaks on the
    admin token comparison (the tenant path uses dict lookup, which is
    already comparison-time-independent for hash-equal keys)."""

    async def admin_auth(
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
    ) -> list[str]:
        admin_key = config.admin_api_key
        if not admin_key:
            # Legacy: no separate admin key set — fall through to tenant
            # auth so existing deployments keep working unchanged.
            return await tenant_auth_dep(authorization=authorization,
                                          x_api_key=x_api_key)
        token = _extract_token(authorization, x_api_key)
        if not token or not hmac.compare_digest(token, admin_key):
            raise HTTPException(status_code=401, detail="invalid admin token")
        # Admin token doesn't map to a user pool — return empty list to
        # match the dependency's declared return type.
        return []
    return admin_auth
