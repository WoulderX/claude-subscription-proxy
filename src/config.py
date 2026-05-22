from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class MitmConfig(BaseModel):
    port_base: int = 18000
    ca_cert: str = "~/.mitmproxy/mitmproxy-ca-cert.pem"


class ClaudeConfig(BaseModel):
    binary: str = "claude"
    home_template: str = "./users/{user_id}"
    # Worker stays alive between requests. To shed accumulated CLI state
    # (Ink scroll buffer, in-memory transcript, cached OAuth access
    # token, any leak) the manager restarts each worker after this many
    # seconds. Keep this comfortably shorter than the OAuth token
    # lifetime (~8h at time of writing): the main-process OAuthRefresher
    # keeps .credentials.json fresh on disk, but workers' in-memory copy
    # only refreshes on restart. If a worker outlives the AT it cached
    # at startup it would try to self-refresh with its now-stale RT and
    # collide with the main-process refresher (the rotated RT is
    # single-use, so one of the two writers gets invalid_grant).
    restart_interval_seconds: int = 14400  # 4h


class Config(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = 8787
    mitm: MitmConfig = Field(default_factory=MitmConfig)
    claude: ClaudeConfig = Field(default_factory=ClaudeConfig)
    # token -> [user_id, ...]. A scalar in YAML (`sk-...: litellm`) is
    # normalised to a single-element list so the rest of the codebase
    # treats every token as a pool. A list (`sk-...: [a, b, c]`) is the
    # pool form — incoming requests for that token are load-balanced
    # across the listed workers.
    users: dict[str, list[str]] = Field(default_factory=dict)

    @field_validator("users", mode="before")
    @classmethod
    def _normalise_users(cls, raw: Any) -> Any:
        if not isinstance(raw, dict):
            return raw
        out: dict[str, list[str]] = {}
        for tok, val in raw.items():
            if isinstance(val, str):
                out[tok] = [val]
            elif isinstance(val, list):
                if not val:
                    raise ValueError(f"users[{tok}] is empty list")
                out[tok] = [str(x) for x in val]
            else:
                raise ValueError(
                    f"users[{tok}] must be str or list of str, got {type(val).__name__}")
        return out

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        data: dict[str, Any] = yaml.safe_load(Path(path).read_text())
        return cls.model_validate(data)

    def user_home(self, user_id: str) -> Path:
        return Path(os.path.expanduser(self.claude.home_template.format(user_id=user_id)))

    def ca_cert_path(self) -> Path:
        return Path(os.path.expanduser(self.mitm.ca_cert))
