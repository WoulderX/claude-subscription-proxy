from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


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


class UsageConfig(BaseModel):
    """Token-usage accounting (the sqlite store behind /admin/usage).
    Disabled by default — set `enabled: true` to start recording. Cost
    of a single event is ~1 sqlite insert, but the db file grows with
    request volume so a deployment that doesn't want the persistence
    overhead can leave it off."""
    enabled: bool = True
    # File path for the sqlite db. Relative paths resolve against the
    # process CWD. In containers this is typically /home/coder/claude-
    # subscription-proxy; mount a host volume here if you want the
    # history to survive image rebuilds. Parent dirs are auto-created.
    db_path: str = "./data/usage.db"


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
    # Route every turn of one conversation to the same account so
    # Anthropic's per-account prompt cache stays hot across the turns
    # (turn N reads what turn N-1 wrote instead of re-creating the whole
    # prefix on a fresh account). Different conversations still spread
    # across accounts, so load stays balanced as long as concurrent
    # conversations outnumber accounts. Falls back to plain round-robin
    # whenever the affinity account is rate-limited or has no usable
    # worker, so toggling this off (then /admin/reload) is always safe.
    session_affinity: bool = True
    # How long an idle conversation keeps its account binding. Refreshed
    # on every request, so an active conversation never expires; a
    # finished one frees its slot after this window.
    session_affinity_ttl_seconds: int = 600
    # Exponential backoff for the "bare rate_limit_error, no reset header"
    # 429 — the shape Anthropic returns on a rolling TPM/RPM spike. These
    # reset in seconds-to-a-minute upstream, so the FIRST cooldown is
    # short (base) and a healthy gap resets it; but if the SAME account
    # keeps re-hitting 429 on each post-cooldown probe, the window
    # doubles (base, 2*base, 4*base, …) up to the cap so we stop hammering
    # a genuinely exhausted account. Parsed-reset / weekly / 5-hour 429s
    # carry an authoritative window and skip this entirely.
    # Default sequence: 120 → 240 → 480 → 600(cap)  (2/4/8/10 min).
    rate_limit_base_cooldown_seconds: int = 120
    rate_limit_max_cooldown_seconds: int = 600
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)


class AccountConfig(BaseModel):
    """One Claude subscription account. `dir` is the host-side directory
    that holds this account's ~/.claude/ contents (.credentials.json,
    .claude.json, projects/, etc.) — mounted into the container and
    symlinked by every worker assigned to this account. `workers` is the
    number of PTYs that will share this account's credentials.

    Worker user_ids are auto-generated as `{account_name}-{0..workers-1}`,
    e.g. account `claude-1` with workers=5 → claude-1-0 .. claude-1-4.
    All workers on the same account share the directory via symlink,
    just like the legacy single-account setup did — the proxy's
    main-process OAuth refresher is the only writer of .credentials.json,
    so the per-account refresh_token rotation stays single-writer.

    `pool` is the front-door sk-key this account belongs to. It exists
    so the dashboard's add-account flow can persist the operator's pool
    choice across container restarts: the wire pass in
    `_wire_accounts` ensures `users[pool]` includes every worker_id of
    accounts tagged with that pool. Optional — accounts without a pool
    tag follow the legacy auto-fill (api_key takes everything unless
    operator carved out users[] explicitly)."""
    dir: str
    workers: int = Field(ge=1)
    pool: str | None = None


class Config(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = 8787
    mitm: MitmConfig = Field(default_factory=MitmConfig)
    claude: ClaudeConfig = Field(default_factory=ClaudeConfig)
    oauth_refresh: OAuthRefreshConfig = Field(default_factory=OAuthRefreshConfig)
    usage: UsageConfig = Field(default_factory=UsageConfig)

    # Multi-account block. When set, each account spawns `workers` PTYs
    # that share that account's .claude/ directory. When absent, the
    # deployment runs in legacy single-account mode where every worker
    # symlinks to the operator's ~/.claude (one global credential).
    accounts: dict[str, AccountConfig] = Field(default_factory=dict)

    # Single front-door key for the multi-account setup. If both
    # `accounts:` and `api_key:` are present, all auto-generated worker
    # user_ids are pooled under this single key. For finer-grained
    # routing (per-tenant keys, mixed pools) use `users:` directly.
    api_key: str | None = None

    # Optional separate token gating /admin/* endpoints (account
    # add/delete, force-refresh, set rate-limit, /admin/usage, etc.).
    # When unset, /admin/* falls back to the tenant key (legacy
    # behavior — any sk- in `users:` works). Set this whenever the
    # tenant key is shared with downstream callers (LiteLLM, OpenAI
    # clients, etc.) so a leaked tenant key can't wipe accounts.
    admin_api_key: str | None = None

    # token -> [user_id, ...]. A scalar in YAML (`sk-...: litellm`) is
    # normalised to a single-element list so the rest of the codebase
    # treats every token as a pool. A list (`sk-...: [a, b, c]`) is the
    # pool form — incoming requests for that token are load-balanced
    # across the listed workers. Can be left empty if `accounts:` +
    # `api_key:` are set; the validator auto-fills it from those.
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

    @model_validator(mode="after")
    def _wire_accounts(self) -> "Config":
        """Cross-field validation + auto-expansion of accounts ↔ users.

        Rules:
          - If `accounts:` is set, auto-generate the canonical worker
            user_id set: {account-0, account-1, ..., account-(N-1)}.
          - If `api_key:` is also set, populate users[api_key] with the
            full auto-generated list (unless the operator already set
            users[api_key] explicitly).
          - If users[] is set explicitly alongside accounts[], every
            referenced user_id must exist in the auto-generated set —
            otherwise the worker would have no account to symlink to
            at runtime.
          - With no `accounts:` block, behavior is legacy single-account:
            users[] is taken at face value and all workers share
            operator ~/.claude."""
        if self.accounts:
            valid_ids: set[str] = set()
            for acc_name, acc in self.accounts.items():
                for i in range(acc.workers):
                    valid_ids.add(f"{acc_name}-{i}")

            # Auto-fill users[] when api_key is set and the operator
            # didn't override users[] for that key explicitly.
            if self.api_key and self.api_key not in self.users:
                self.users[self.api_key] = sorted(valid_ids)

            # Per-account `pool` tag application. Used by the dashboard
            # add-account flow to persist "which sk-key does this new
            # account live under" across restarts, without forcing the
            # operator to hand-edit config.yaml's users[] every time.
            # Idempotent: if the pool already lists the account's
            # worker_ids (operator did it explicitly in config.yaml),
            # this is a no-op for those entries.
            for acc_name, acc in self.accounts.items():
                if not acc.pool:
                    continue
                if acc.pool not in self.users:
                    raise ValueError(
                        f"accounts[{acc_name}].pool={acc.pool!r} refers to "
                        f"an sk-key not present in users[]; add the key to "
                        f"users[] in config.yaml or clear the pool tag")
                existing = list(self.users[acc.pool])
                seen = set(existing)
                for i in range(acc.workers):
                    uid = f"{acc_name}-{i}"
                    if uid not in seen:
                        existing.append(uid)
                        seen.add(uid)
                self.users[acc.pool] = existing

            # Validate every referenced user_id resolves to an account
            referenced: set[str] = set()
            for pool in self.users.values():
                referenced.update(pool)
            unknown = referenced - valid_ids
            if unknown:
                raise ValueError(
                    f"users[] references unknown user_ids {sorted(unknown)}; "
                    f"with `accounts:` set, every user_id must be of the form "
                    f"`<account>-<index>` where <account> is in "
                    f"{sorted(self.accounts.keys())} and <index> < "
                    f"accounts[<account>].workers")

        if not self.users:
            raise ValueError(
                "no users configured — set either `accounts:` + `api_key:`, "
                "or `users:` directly")
        return self

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        """Load config.yaml. If a sibling runtime file exists at the
        path returned by `runtime_accounts_path_for(config)`, its
        `accounts:` block is merged on top of the static config — keys
        in both files come from runtime (operator can override via
        config.yaml by removing the runtime entry).

        The runtime file is where dashboard-driven account additions
        get persisted, so config.yaml stays read-only and pristine."""
        data: dict[str, Any] = yaml.safe_load(Path(path).read_text())
        # Runtime accounts overlay. Path is derived from usage.db_path's
        # parent (which is the RW-mounted /data/proxy/), with a fixed
        # filename. Falls back silently when the file doesn't exist —
        # cold-start runs through config.yaml alone, same as before.
        runtime_path = _runtime_accounts_path(data)
        if runtime_path is not None and runtime_path.is_file():
            try:
                runtime_data = yaml.safe_load(runtime_path.read_text()) or {}
            except yaml.YAMLError:
                runtime_data = {}
            runtime_accounts = runtime_data.get("accounts")
            if isinstance(runtime_accounts, dict):
                merged_accounts = dict(data.get("accounts") or {})
                merged_accounts.update(runtime_accounts)
                data["accounts"] = merged_accounts
                # If api_key is set and users[] wasn't explicitly
                # carved out, expand the auto-populated pool to cover
                # the newly merged accounts. Without this, the
                # validator would still build users[api_key] from the
                # full account set, but only if users[api_key] is
                # absent — operator overrides win.
        return cls.model_validate(data)

    def user_home(self, user_id: str) -> Path:
        return Path(os.path.expanduser(self.claude.home_template.format(user_id=user_id)))

    def ca_cert_path(self) -> Path:
        return Path(os.path.expanduser(self.mitm.ca_cert))

    def account_for_user(self, user_id: str) -> AccountConfig | None:
        """Resolve a worker user_id back to its AccountConfig. Returns
        None in legacy mode (no `accounts:` block) — callers fall back
        to operator ~/.claude in that case.

        Lookup is by exact-prefix match against configured account names
        rather than naive string-split, because account names themselves
        may contain hyphens (e.g. `team-a-prod` with workers ≥ 1 gives
        `team-a-prod-0`; we need to peel exactly one trailing `-N` off
        the end, NOT split on first `-`)."""
        if not self.accounts:
            return None
        # Tail must be `-<digits>`; everything before is the account name.
        sep = user_id.rfind("-")
        if sep <= 0:
            return None
        head, tail = user_id[:sep], user_id[sep + 1:]
        if not tail.isdigit():
            return None
        return self.accounts.get(head)

    def account_dir(self, account_name: str) -> Path:
        """Resolved on-disk path of an account's .claude/ contents dir."""
        return Path(os.path.expanduser(self.accounts[account_name].dir))

    def credentials_paths(self) -> list[Path]:
        """Every account's .credentials.json, in account-name order. In
        legacy mode this is the single operator credentials path."""
        if not self.accounts:
            return [Path(os.path.expanduser("~")) / ".claude" / ".credentials.json"]
        return [
            self.account_dir(name) / ".credentials.json"
            for name in sorted(self.accounts.keys())
        ]

    def runtime_accounts_path(self) -> Path | None:
        """Where dashboard-added accounts get persisted. Lives alongside
        the usage sqlite db (already RW-mounted) so we don't need a
        separate writable volume. None if usage tracking is disabled —
        in that case dynamic accounts are in-memory only and lost on
        restart (a deliberate degradation, not a hard error)."""
        if not self.usage.db_path:
            return None
        return Path(self.usage.db_path).parent / "accounts.runtime.yaml"

    def write_runtime_accounts(self) -> None:
        """Serialise self.accounts into the runtime overlay file. Writes
        the FULL accounts dict — load() merges runtime on top of static
        config.yaml, so anything in runtime wins. Operator-edited
        accounts in config.yaml that ALSO appear in runtime would get
        runtime's values; remove from runtime to fall back to static.

        The `pool` field is included only when set; this keeps the
        overlay diff minimal for accounts that don't carry a pool
        tag (most pre-existing accounts written by older code)."""
        path = self.runtime_accounts_path()
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        accounts_out: dict[str, dict[str, Any]] = {}
        for name, acc in self.accounts.items():
            entry: dict[str, Any] = {"dir": acc.dir, "workers": acc.workers}
            if acc.pool:
                entry["pool"] = acc.pool
            accounts_out[name] = entry
        payload = {"accounts": accounts_out}
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(yaml.safe_dump(payload, allow_unicode=True,
                                       sort_keys=True))
        tmp.replace(path)


def _runtime_accounts_path(data: dict) -> Path | None:
    """Module-level twin of `Config.runtime_accounts_path` for use
    during `load()` — we need to know the path BEFORE the Config
    instance exists. Reads usage.db_path out of the raw dict; falls
    back to /data/proxy/accounts.runtime.yaml (the docker-compose
    default) when usage block is missing."""
    usage = data.get("usage") if isinstance(data, dict) else None
    db_path: str | None = None
    if isinstance(usage, dict):
        db_path = usage.get("db_path") if isinstance(usage.get("db_path"), str) else None
    if not db_path:
        db_path = "/data/proxy/usage.db"
    return Path(db_path).parent / "accounts.runtime.yaml"
