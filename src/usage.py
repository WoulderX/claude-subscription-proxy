"""Token usage accounting.

Every completed /v1/messages call emits a structured usage event:

  {
    "account":      "claude-1",        # null in legacy single-account
    "worker":       "claude-1-3",      # user_id of the worker that served it
    "litellm_user": "alice",           # x-litellm-user-id forwarded by LiteLLM
    "model":        "claude-opus-4-5",
    "input_tokens": 15,
    "output_tokens": 284,
    "cache_creation_tokens": 0,
    "cache_read_tokens": 12000,
    "ts":           1748039201.4
  }

UsageStore persists these to sqlite so the /admin/usage endpoint can
answer per-account / per-worker / per-litellm-user breakdowns across a
configurable time range (current worker lifecycle, today, last 7 days)
without consulting the running worker state.

Sqlite was chosen over JSONL or in-memory:
  - Survives container restart — "last 7 days" stays meaningful after
    a redeploy.
  - GROUP BY + WHERE ts > X are exactly the queries we need; full-scan
    is fine at our event rate (one row per completed request).
  - Single-writer (main process) so we don't need WAL gymnastics.

PRICING is the published Anthropic API price list, used only to estimate
a USD figure for operator dashboards. Subscription accounts are billed
flat-rate; the estimate exists so operators can compare workloads to
"what would this cost on the API" — never displayed as actual spend.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# Anthropic public pricing as of 2026-05 (USD per 1M tokens). Lookup is
# prefix-based — keys are matched longest-first against the model name
# so `claude-opus-4-7-20260101` falls back to `claude-opus-4` etc.
# Update when Anthropic ships a new pricing tier; missing models yield
# estimated_usd = null in API responses (the UI just shows a dash).
PRICING: dict[str, dict[str, float]] = {
    # Opus 4.x
    "claude-opus-4": {
        "input": 5.00, "output": 25.00,
        "cache_write": 6.25, "cache_read": 0.50,
    },
    # Sonnet 4.x
    "claude-sonnet-4": {
        "input": 3.00, "output": 15.00,
        "cache_write": 3.75, "cache_read": 0.30,
    },
    # Haiku 4.x
    "claude-haiku-4": {
        "input": 1.00, "output": 5.00,
        "cache_write": 1.25, "cache_read": 0.10,
    },
}


def estimate_usd(model: str | None, *,
                 input_tokens: int = 0,
                 output_tokens: int = 0,
                 cache_creation_tokens: int = 0,
                 cache_read_tokens: int = 0) -> float | None:
    """Compute the API-equivalent USD cost for this token mix.
    Returns None when the model is unknown so the UI can show a dash
    rather than a misleading zero (zero would imply "free" — wrong)."""
    if not model:
        return None
    p = _pricing_for(model)
    if p is None:
        return None
    return (
        input_tokens          * p["input"]       / 1_000_000.0
        + output_tokens         * p["output"]      / 1_000_000.0
        + cache_creation_tokens * p["cache_write"] / 1_000_000.0
        + cache_read_tokens     * p["cache_read"]  / 1_000_000.0
    )


def _pricing_for(model: str) -> dict[str, float] | None:
    """Longest-prefix lookup so a versioned model name like
    `claude-opus-4-5-20260101` still resolves to the `claude-opus-4`
    tier. Returns None for unknown families."""
    candidates = [k for k in PRICING if model.startswith(k)]
    if not candidates:
        return None
    candidates.sort(key=len, reverse=True)
    return PRICING[candidates[0]]


class UsageStore:
    """Sqlite-backed log of completed-request token usage.

    Writes happen on the main asyncio loop from SessionManager._record_usage.
    sqlite3 connection objects are not thread-safe by default; we serialise
    writes through a Lock so a future move to a background flusher (or a
    test harness that calls record() from a different thread) won't blow
    up. Reads use a fresh connection per call — sqlite handles concurrent
    readers natively.

    The schema is intentionally event-stream rather than pre-aggregated:
    individual events let us compute new groupings later without touching
    historical data. At ~1 row per request, even a busy proxy generates
    far less than a million rows per week (well within sqlite's comfort
    zone)."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()
        log.info("usage store ready at %s", self.db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path),
                               isolation_level=None,  # autocommit
                               timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS usage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    account TEXT,
                    worker TEXT NOT NULL,
                    litellm_user TEXT,
                    model TEXT,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
                    cache_read_tokens INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_events(ts);
                CREATE INDEX IF NOT EXISTS idx_usage_account_ts
                    ON usage_events(account, ts);
                CREATE INDEX IF NOT EXISTS idx_usage_worker_ts
                    ON usage_events(worker, ts);
            """)

    def record(self, *, ts: float, account: str | None, worker: str,
               litellm_user: str | None, model: str | None,
               input_tokens: int, output_tokens: int,
               cache_creation_tokens: int, cache_read_tokens: int) -> None:
        """Append one usage event. Best-effort: any sqlite failure is
        logged but does not propagate — accounting is observability,
        not load-bearing, so we never want a corrupt db file to take
        down request handling."""
        try:
            with self._lock, self._connect() as conn:
                conn.execute(
                    "INSERT INTO usage_events "
                    "(ts, account, worker, litellm_user, model, "
                    "input_tokens, output_tokens, "
                    "cache_creation_tokens, cache_read_tokens) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (ts, account, worker, litellm_user, model,
                     int(input_tokens), int(output_tokens),
                     int(cache_creation_tokens), int(cache_read_tokens)))
        except sqlite3.Error:
            log.exception("usage record write failed (event dropped)")

    def query(self, *, since: float, until: float | None,
              group_by: str) -> list[dict[str, Any]]:
        """Return aggregated token counts grouped by `group_by`. The
        group key column name on the returned rows is always "key" so
        the API layer doesn't have to switch on group_by. Rows with a
        null grouping value (e.g. legacy single-account events with
        account=NULL) come back with key="" — easier to handle than
        null in the UI."""
        if group_by not in _GROUP_COLUMNS:
            raise ValueError(
                f"group_by must be one of {list(_GROUP_COLUMNS)}, "
                f"got {group_by!r}")
        col = _GROUP_COLUMNS[group_by]
        params: list[Any] = [since]
        where = "ts >= ?"
        if until is not None:
            where += " AND ts < ?"
            params.append(until)
        sql = (
            f"SELECT COALESCE({col}, '') AS key, "
            "SUM(input_tokens) AS input_tokens, "
            "SUM(output_tokens) AS output_tokens, "
            "SUM(cache_creation_tokens) AS cache_creation_tokens, "
            "SUM(cache_read_tokens) AS cache_read_tokens, "
            "COUNT(*) AS request_count "
            f"FROM usage_events WHERE {where} "
            f"GROUP BY COALESCE({col}, '') "
            "ORDER BY key ASC"
        )
        with self._connect() as conn:
            cur = conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def query_by_model(self, *, since: float, until: float | None,
                       group_by: str, key: str) -> list[dict[str, Any]]:
        """Per-model breakdown within a single group bucket. Used by the
        UI's per-row "expand" affordance — the operator clicks a row,
        we list the models that contributed to it."""
        if group_by not in _GROUP_COLUMNS:
            raise ValueError(f"bad group_by {group_by!r}")
        col = _GROUP_COLUMNS[group_by]
        params: list[Any] = [since, key]
        where = "ts >= ? AND COALESCE(" + col + ", '') = ?"
        if until is not None:
            where += " AND ts < ?"
            params.append(until)
        sql = (
            "SELECT COALESCE(model, '') AS model, "
            "SUM(input_tokens) AS input_tokens, "
            "SUM(output_tokens) AS output_tokens, "
            "SUM(cache_creation_tokens) AS cache_creation_tokens, "
            "SUM(cache_read_tokens) AS cache_read_tokens, "
            "COUNT(*) AS request_count "
            f"FROM usage_events WHERE {where} "
            "GROUP BY model ORDER BY model ASC"
        )
        with self._connect() as conn:
            cur = conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


_GROUP_COLUMNS = {
    "account": "account",
    "worker": "worker",
    "litellm_user": "litellm_user",
}
