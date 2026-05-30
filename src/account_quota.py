"""Anthropic 官方订阅 OAuth 用量查询（5h / 7d 滚动窗口）。

调用未公开 endpoint `GET https://api.anthropic.com/api/oauth/usage`，
用本 host 上 ~/.claude/.credentials.json 里的 OAuth access_token 鉴权。
端口、字段名、beta header 均参考 cc-switch 项目（farion1231/cc-switch）。

本服务全局只有一个实例（账号是 host 级别共享，不是 per-worker），结果
缓存 + 60 秒最小刷新间隔——防止前端误触把上游打满。前端"立即刷新"按
钮的频控也以此为准（服务端是唯一可信来源）。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
BETA_HEADER = "oauth-2025-04-20"
HTTP_TIMEOUT_SECONDS = 10.0

# 防止前端按钮误触造成对上游高频请求。Anthropic 没公开这个 endpoint 的
# rate limit；保守取 60 秒——5h/7d 窗口本身就是分钟级粒度，再快也没用。
MIN_REFRESH_INTERVAL_SECONDS = 60.0

# 上游返回 429 时，如果 Anthropic 没给 Retry-After，就用这个兜底等待时长。
# 该 endpoint 的实际限速 cc-switch 也没文档化；实测一次 429 后短期内
# 持续被限速，5 分钟是观察到的平均恢复时长。
DEFAULT_429_BACKOFF_SECONDS = 300.0


@dataclass
class QuotaSnapshot:
    """一次成功的 oauth/usage 抓取结果。错误不写到这里——失败的尝试不
    会覆盖上一次成功的数据，避免 dashboard 因为一次 429 就丢掉历史可见性。"""
    fetched_at_unix: float                    # 服务端 wall-clock 时间戳（前端显示用）
    five_hour: dict[str, Any] | None = None
    seven_day: dict[str, Any] | None = None
    seven_day_opus: dict[str, Any] | None = None
    seven_day_sonnet: dict[str, Any] | None = None
    extra_usage: dict[str, Any] | None = None
    extra_tiers: dict[str, Any] = field(default_factory=dict)  # API 未来加新窗口的兜底


@dataclass
class QuotaAttemptError:
    """最近一次失败的尝试。下一次成功后清空。"""
    attempted_at_unix: float
    error: str
    credential_status: str = "valid"          # valid / expired / not_found / parse_error
    http_status: int | None = None
    is_rate_limited: bool = False             # 上游 429
    retry_after_seconds: float | None = None  # 上游告诉我们等多久


class AccountQuotaService:
    """全局唯一。所有线程/协程共用一份 snapshot + 一份 rate-limit gate。"""

    def __init__(self, credentials_path: Path) -> None:
        self.credentials_path = credentials_path
        self._lock = asyncio.Lock()                       # 串行化 refresh，避免并发打到上游
        self._snapshot: QuotaSnapshot | None = None      # 上次成功结果
        self._last_error: QuotaAttemptError | None = None  # 上次失败原因（成功后清空）
        # monotonic 用于频控（不受系统时钟跳变影响）
        self._last_fetch_monotonic: float = 0.0
        # 429 触发的 backoff（≥ 默认 60s 频控的更长禁令）
        self._cooldown_until_monotonic: float = 0.0

    # ── 对外接口 ──────────────────────────────────────────

    def get_state(self) -> dict[str, Any]:
        """读当前状态。不触发任何网络请求；前端用这个画图。"""
        return self._state_dict(rate_limited=False)

    async def refresh(self) -> dict[str, Any]:
        """主动刷新。如果还在 cooldown 内（普通 60s 或 429 backoff），
        不重新抓取，直接返回当前状态 + rate_limited=true。"""
        async with self._lock:
            now = time.monotonic()
            seconds_remaining = self._cooldown_seconds_remaining(now)
            # 没拿过任何数据时，第一次允许直接打——但若上次刚 429 也得等
            never_fetched = self._last_fetch_monotonic == 0.0
            if seconds_remaining > 0 and not never_fetched:
                return self._state_dict(rate_limited=True)
            await self._do_fetch()
            return self._state_dict(rate_limited=False)

    # ── 内部 ──────────────────────────────────────────────

    def _cooldown_seconds_remaining(self, now_monotonic: float) -> float:
        """综合普通 60s 频控 + 429 backoff，取两者中更晚的解禁时间。"""
        if self._last_fetch_monotonic == 0.0:
            return 0.0
        regular_unblock = self._last_fetch_monotonic + MIN_REFRESH_INTERVAL_SECONDS
        unblock = max(regular_unblock, self._cooldown_until_monotonic)
        return max(0.0, unblock - now_monotonic)

    def _state_dict(self, *, rate_limited: bool) -> dict[str, Any]:
        snap = asdict(self._snapshot) if self._snapshot is not None else None
        err = asdict(self._last_error) if self._last_error is not None else None
        seconds_until_next = self._cooldown_seconds_remaining(time.monotonic())
        return {
            "snapshot": snap,
            "last_error": err,
            "rate_limited": rate_limited,
            "rate_limit_seconds": MIN_REFRESH_INTERVAL_SECONDS,
            "seconds_until_next_refresh": round(seconds_until_next, 1),
        }

    def _read_access_token(self) -> tuple[str | None, str, str | None]:
        """返回 (access_token, credential_status, error_message)。

        优先级和 cc-switch 一致：先文件（容器/Linux），macOS Keychain
        我们用不到（这服务跑在 Linux 容器里），略。"""
        path = self.credentials_path
        if not path.is_file():
            return None, "not_found", f"credentials file not found: {path}"
        try:
            raw = path.read_text()
        except OSError as e:
            return None, "parse_error", f"read failed: {e}"
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            return None, "parse_error", f"invalid JSON: {e}"
        entry = data.get("claudeAiOauth") or data.get("claude.ai_oauth")
        if not isinstance(entry, dict):
            return None, "parse_error", "no claudeAiOauth entry"
        token = entry.get("accessToken")
        if not isinstance(token, str) or not token:
            return None, "parse_error", "accessToken missing/empty"
        # 不主动判断 expiresAt——OAuthRefresher 会保证 token fresh；
        # 真过期了上游会返回 401，我们捕获处理。
        return token, "valid", None

    async def _do_fetch(self) -> None:
        """实际拉取。成功时覆盖 _snapshot 并清空 _last_error；失败时
        只写 _last_error，保留上次成功的 snapshot（dashboard 不会因
        一次 429 就丢掉历史可见性）。"""
        now_unix = time.time()
        token, cred_status, cred_err = self._read_access_token()
        if token is None:
            self._record_error(QuotaAttemptError(
                attempted_at_unix=now_unix,
                error=cred_err or "credentials missing",
                credential_status=cred_status,
            ))
            return

        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
                resp = await client.get(USAGE_URL, headers={
                    "Authorization": f"Bearer {token}",
                    "anthropic-beta": BETA_HEADER,
                    "Accept": "application/json",
                })
        except httpx.HTTPError as e:
            self._record_error(QuotaAttemptError(
                attempted_at_unix=now_unix,
                error=f"network error: {e}",
                credential_status="valid",
            ))
            log.warning("account quota fetch network error: %s", e)
            return

        if resp.status_code in (401, 403):
            self._record_error(QuotaAttemptError(
                attempted_at_unix=now_unix,
                error=f"OAuth 鉴权失败 HTTP {resp.status_code}——access_token 已失效",
                credential_status="expired",
                http_status=resp.status_code,
            ))
            log.warning("account quota fetch 401/403; token expired")
            return

        if resp.status_code == 429:
            retry_after = _parse_retry_after(resp.headers.get("retry-after"))
            backoff = retry_after if retry_after is not None else DEFAULT_429_BACKOFF_SECONDS
            self._cooldown_until_monotonic = time.monotonic() + backoff
            self._record_error(QuotaAttemptError(
                attempted_at_unix=now_unix,
                error=(f"/api/oauth/usage 探针端点限速 HTTP 429——已 backoff "
                       f"{int(backoff)} 秒（{'按 Retry-After' if retry_after else '默认'}）。"
                       f"该端点限流独立（~1 req/h/账号），仅影响 dashboard "
                       f"用量百分比读取，不代表账号 chat 受限。"
                       f"原始响应：{resp.text[:200]}"),
                credential_status="valid",
                http_status=429,
                is_rate_limited=True,
                retry_after_seconds=backoff,
            ))
            log.warning("account quota 429; backoff %ss (retry_after=%s)",
                        backoff, retry_after)
            return

        if resp.status_code != 200:
            self._record_error(QuotaAttemptError(
                attempted_at_unix=now_unix,
                error=f"上游 HTTP {resp.status_code}: {resp.text[:300]}",
                credential_status="valid",
                http_status=resp.status_code,
            ))
            log.warning("account quota fetch HTTP %s: %s",
                        resp.status_code, resp.text[:200])
            return

        try:
            body = resp.json()
        except ValueError as e:
            self._record_error(QuotaAttemptError(
                attempted_at_unix=now_unix,
                error=f"响应非 JSON: {e}",
                credential_status="valid",
                http_status=200,
            ))
            return

        known = ("five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet")
        kwargs: dict[str, Any] = {k: body.get(k) for k in known}
        extra_usage = body.get("extra_usage") if isinstance(body, dict) else None
        extra_tiers: dict[str, Any] = {}
        if isinstance(body, dict):
            for k, v in body.items():
                if k in known or k == "extra_usage":
                    continue
                if isinstance(v, dict) and "utilization" in v:
                    extra_tiers[k] = v

        self._snapshot = QuotaSnapshot(
            fetched_at_unix=now_unix,
            extra_usage=extra_usage if isinstance(extra_usage, dict) else None,
            extra_tiers=extra_tiers,
            **kwargs,
        )
        self._last_error = None
        self._last_fetch_monotonic = time.monotonic()
        # 成功后解除 429 backoff（如果之前有）
        self._cooldown_until_monotonic = 0.0
        log.info("account quota fetched: 5h=%s%% 7d=%s%%",
                 (kwargs.get("five_hour") or {}).get("utilization"),
                 (kwargs.get("seven_day") or {}).get("utilization"))

    def _record_error(self, err: QuotaAttemptError) -> None:
        """统一记录失败：不动 _snapshot（保留上次成功的可见性），只更
        新 _last_error 和 _last_fetch_monotonic 让 60s 频控生效。"""
        self._last_error = err
        self._last_fetch_monotonic = time.monotonic()


def _parse_retry_after(raw: str | None) -> float | None:
    """Retry-After 既可以是秒数，也可以是 HTTP-date。我们只处理前者
    (Anthropic 实测返回的是数字秒)，HTTP-date 路径用默认 backoff 兜底。"""
    if not raw:
        return None
    try:
        v = float(raw.strip())
        return v if v > 0 else None
    except ValueError:
        return None
