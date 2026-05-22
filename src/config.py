from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class MitmConfig(BaseModel):
    port_base: int = 18000
    ca_cert: str = "~/.mitmproxy/mitmproxy-ca-cert.pem"


class TimeoutConfig(BaseModel):
    """Operational tuning knobs for the various worker lifecycle phases.
    Defaults match the values that were previously hardcoded across the
    codebase. Adjust if you see corresponding failures under your real
    workload — otherwise leave alone."""
    # How long the worker's mitm has to intercept the TUI's outbound
    # /v1/messages call. If exceeded, the channel is closed and the
    # request fails. 90s default rather than 30s because after a long
    # sub-agent burst (e.g. CC /explore with 30+ tool uses), claude CLI
    # spends time on local work — auto-compact, transcript indexing,
    # MCP tool execution — and the next PTY trigger gets ignored until
    # that local work finishes. 30s was clipping legitimate "still
    # thinking, give me a moment" states.
    mitm_intercept_seconds: float = 90.0
    # /status marks an in-flight request as "stuck" if it has gone this
    # long without receiving a single byte. Anthropic typically emits a
    # chunk every couple of seconds, so 30s is a conservative gap. Raise
    # if you run prompts that genuinely produce long initial latency
    # (very large system prompt + reasoning models).
    status_stall_seconds: float = 30.0
    # The mitm response-stream watchdog auto-closes a channel that has
    # gone this long without a new chunk. Defends against the pattern
    # where Anthropic sends a small non-SSE error body (e.g. a
    # rate_limit_error JSON ~150 bytes) and then leaves the connection
    # idle without a clean end-of-stream signal — the channel would
    # otherwise hang in the worker forever, leaving the worker "busy"
    # from /status's POV even though there's no actual work happening.
    # Strictly more conservative than status_stall_seconds so a slow-
    # but-real reasoning model isn't killed prematurely.
    response_stall_seconds: float = 90.0
    # How long the manager will wait for in-flight streams to drain
    # during a scheduled restart before tearing the worker down. Raise
    # if your typical requests take longer than this.
    restart_drain_seconds: float = 60.0
    # How long a fresh worker has to signal {"type":"ready"} on stdout
    # before we kill it. Raise if cold-start is slow (busy host, slow
    # mitm bring-up).
    worker_ready_seconds: float = 60.0
    # How long bootstrap prewarm can take before we give up and let the
    # worker serve real traffic anyway. Non-fatal — the worker is still
    # usable; the first real request may just hit a rate limit and need
    # to be retried.
    prewarm_seconds: float = 60.0
    # How often the manager polls all workers for age-based scheduled
    # restart. Smaller = quicker reaction to restart_interval expiry but
    # higher idle CPU; larger = workers may run up to this much past
    # restart_interval before being recycled.
    restart_check_interval_seconds: float = 60.0


class OAuthRefreshConfig(BaseModel):
    """Centralised proactive OAuth token refresh. With this enabled, the
    main process is the sole writer of /v1/oauth/token requests, which
    eliminates the multi-worker refresh_token rotation race (workers'
    in-memory tokens stay valid because they get scheduled-restarted
    inside the token lifetime via claude.restart_interval_seconds, and
    they re-read fresh creds from the shared .credentials.json on each
    restart). Disable only for debugging — with this off, workers fall
    back to self-refresh and the race can re-emerge."""
    enabled: bool = True
    # How often to check the credentials file's expiry timestamp.
    check_interval_seconds: float = 300.0
    # If the token will expire within this window, refresh now.
    refresh_when_expires_within_seconds: float = 3600.0


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
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)


class Config(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = 8787
    mitm: MitmConfig = Field(default_factory=MitmConfig)
    claude: ClaudeConfig = Field(default_factory=ClaudeConfig)
    oauth_refresh: OAuthRefreshConfig = Field(default_factory=OAuthRefreshConfig)
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
