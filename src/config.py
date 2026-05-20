from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class MitmConfig(BaseModel):
    port_base: int = 18000
    ca_cert: str = "~/.mitmproxy/mitmproxy-ca-cert.pem"


class ClaudeConfig(BaseModel):
    binary: str = "claude"
    home_template: str = "./users/{user_id}"
    # Worker stays alive between requests. To shed accumulated CLI state
    # (Ink scroll buffer, in-memory transcript, cached OAuth access token,
    # any leak) the manager restarts each worker after this many seconds.
    restart_interval_seconds: int = 43200  # 12h


class Config(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = 8787
    mitm: MitmConfig = Field(default_factory=MitmConfig)
    claude: ClaudeConfig = Field(default_factory=ClaudeConfig)
    users: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        data: dict[str, Any] = yaml.safe_load(Path(path).read_text())
        return cls.model_validate(data)

    def user_home(self, user_id: str) -> Path:
        return Path(os.path.expanduser(self.claude.home_template.format(user_id=user_id)))

    def ca_cert_path(self) -> Path:
        return Path(os.path.expanduser(self.mitm.ca_cert))
