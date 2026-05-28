"""Per-user session = a `src.worker` subprocess holding its own mitm +
claude PTY. The server talks to it over stdin/stdout JSON lines."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Callable

from ..config import Config
from ..rate_limit import (
    classify_rate_limit_reason,
    extract_reset_from_response,
    parse_reset_time,
)
from .state import ResponseChannel

log = logging.getLogger(__name__)


def _summarize_body(
    body: dict[str, Any],
    request_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Produce a small log-safe summary of a request body for /status,
    so operators can see what a worker is processing without expanding
    the full JSON. Truncates user content to 80 chars to bound response
    size and avoid dumping full prompts into a health endpoint.

    request_metadata is merged into the top-level summary (typically
    holds {"litellm": {...}} extracted from x-litellm-* request headers
    so the operator can attribute the in-flight task to the original
    LiteLLM virtual user — by default the proxy only sees LiteLLM's
    upstream API key, not the end user behind it)."""
    if not isinstance(body, dict):
        return dict(request_metadata) if request_metadata else {}
    msgs = body.get("messages") if isinstance(body.get("messages"), list) else []
    last_user_text = ""
    for m in reversed(msgs):
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        content = m.get("content")
        if isinstance(content, str):
            last_user_text = content
        elif isinstance(content, list):
            for blk in content:
                if isinstance(blk, dict) and blk.get("type") == "text":
                    last_user_text = blk.get("text", "")
                    break
        break
    # Strip Claude Code's `<system-reminder>...</system-reminder>`
    # blocks before truncating. The CLI injects them into the last
    # user message to carry harness metadata (available skills, cwd,
    # permission mode, etc.) — useful upstream but pure noise on the
    # /status preview, where the operator wants to see the actual
    # natural-language intent of the request. Multiple reminder blocks
    # can appear; strip them ALL, then collapse runs of whitespace
    # left behind so the surviving prose reads naturally.
    cleaned = re.sub(
        r"<system-reminder>.*?</system-reminder>",
        "", last_user_text, flags=re.DOTALL)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    # 留够多让 /ui 的 hover tooltip 能显示完整消息体；上限是为了避免
    # 把整段 system prompt / 多 KB 用户输入塞进 /status 响应。
    _PREVIEW_LIMIT = 2000
    preview = cleaned[:_PREVIEW_LIMIT]
    if len(cleaned) > _PREVIEW_LIMIT:
        preview += "…"
    # Empty after stripping means the entire user message was just
    # reminder boilerplate (Claude Code's startup probes do this).
    # Show a placeholder so the column isn't blank and operators can
    # tell it's a no-content request rather than a missing summary.
    if not preview:
        preview = "(仅 system-reminder, 无用户正文)"
    summary: dict[str, Any] = {
        "model": body.get("model"),
        "max_tokens": body.get("max_tokens"),
        "stream": bool(body.get("stream")),
        "n_messages": len(msgs),
        "last_user_preview": preview,
    }
    if request_metadata:
        summary.update(request_metadata)
    return summary


class ClaudeSession:
    """Owns the worker subprocess for one user. Serialises requests with
    a per-session asyncio lock — one outbound /v1/messages per claude
    process at a time."""

    def __init__(self, user_id: str, mitm_port: int, config: Config,
                 on_rate_limit: Callable[..., None] | None = None,
                 on_usage: Callable[[str, dict], None] | None = None,
                 on_quota: Callable[[str, dict, float], None] | None = None,
                 on_quota_429: Callable[[str, float | None, float], None] | None = None) -> None:
        self.user_id = user_id
        self.mitm_port = mitm_port
        self.config = config
        self.lock = asyncio.Lock()  # one in-flight request per user
        self.last_used = time.monotonic()
        self.started_at = time.monotonic()
        # Wall-clock variant of started_at. Used to scope the /admin/usage
        # "current worker lifecycle" range: we filter the sqlite event
        # log by ts >= started_at_wall. Refreshed by restart().
        self.started_at_wall = time.time()

        self.proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._next_req_id = 0
        self._channels: dict[int, ResponseChannel] = {}
        self._closed = False
        # Timestamp (monotonic) of the most-recent response-channel
        # close. Used by SessionManager.pick() as a "TUI cooldown"
        # signal: even though _channels is empty the moment we put(None)
        # to the channel, the claude TUI may still be rendering the
        # tail of the response stream (spinner animation, tool-permission
        # modal, etc.) for ~1-2s afterwards. Picking a worker that just
        # closed its channel makes trigger() type the placeholder INTO
        # that lingering TUI state — observed as mitm-intercept timeouts
        # with screens full of "(1m 0s · ↑ 156 tokens)" / spinner dots.
        # `None` means "never finished a request" (fresh worker).
        self._last_channel_close_at: float | None = None
        # Consecutive intercept-timeout failures (worker received our
        # trigger keystroke but mitm never saw the outbound /v1/messages
        # within mitm_intercept_seconds → channel closed with zero
        # bytes). A single failure can be transient (claude CLI mid-tool-
        # chain, brief PTY buffer hiccup) but two in a row signals the
        # worker is genuinely stuck (V8 hang, PTY corruption, modal we
        # can't dismiss). When the counter hits _FORCE_RESTART_THRESHOLD
        # mark_intercept_failure() schedules a force-restart so the
        # worker self-heals within seconds instead of waiting for the
        # 4h scheduled restart. Cleared by mark_request_success().
        self.consecutive_intercept_failures: int = 0
        # In-flight guard so two near-simultaneous failures don't queue
        # two restart tasks (the second would wait on self.lock that
        # the first holds, then attempt a double-restart). Cleared in
        # the restart task's finally block.
        self._force_restart_in_progress: bool = False
        # Periodic self-test so stuck workers are caught BEFORE the
        # next real user request lands on them. See _keepalive_loop()
        # for the full rationale; the task is started by start() and
        # cancelled by stop()/restart().
        self._keepalive_task: asyncio.Task | None = None
        # Called when we spot `rate_limit_error` in the head of an SSE
        # response: signature (user_id, reason, window_seconds). The
        # manager wires this up to its per-account rate-limit table so
        # subsequent routing skips this worker's account until the
        # window expires.
        self._on_rate_limit = on_rate_limit
        # Called when a request completes with parsed token usage:
        # signature (user_id, usage_payload). Manager wires this to its
        # UsageStore; the payload includes the litellm user id from
        # the channel's body_summary so the store row gets correctly
        # tagged. None disables accounting (usage:enabled=false).
        self._on_usage = on_usage
        # Called when the mitm addon captures a /api/oauth/usage response:
        # signature (user_id, body, fetched_at_unix). Manager fans this
        # out to QuotaProbeService keyed by the worker's account.
        self._on_quota = on_quota
        # Same plumbing for the 429 case — manager forwards to the
        # service's record_429() so the account enters a cooldown.
        # signature: (user_id, retry_after_seconds_or_none, fetched_at_unix).
        self._on_quota_429 = on_quota_429

    async def start(self) -> None:
        home = self.config.user_home(self.user_id).resolve()
        home.mkdir(parents=True, exist_ok=True)
        self._seed_home(home)

        worker_env = os.environ.copy()
        # Worker emits structured logs on stderr; keep level configurable
        # via the same env var the server uses.
        worker_env["PYTHONUNBUFFERED"] = "1"

        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", "-m", "src.worker",
            "--user-id", self.user_id,
            "--mitm-port", str(self.mitm_port),
            "--home", str(home),
            "--ca-cert", str(self.config.ca_cert_path()),
            "--claude-binary", self.config.claude.binary,
            "--log-level", os.environ.get("LOG_LEVEL", "INFO"),
            "--mitm-intercept-timeout",
                str(self.config.claude.timeouts.mitm_intercept_seconds),
            "--response-stall-timeout",
                str(self.config.claude.timeouts.response_stall_seconds),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=None,  # inherit — worker logs flow to server stderr
            env=worker_env,
            # Worker streams SSE chunks back as base64-wrapped JSON lines.
            # A single Anthropic chunk over ~48 KB raw bytes (≈ 64 KB after
            # base64 + JSON envelope) overflows the default StreamReader
            # buffer, _read_loop raises LimitOverrunError, the read coroutine
            # dies and the worker process exits. Large tool_use inputs and
            # long text_delta blocks hit this regularly. 16 MiB has room
            # for anything Anthropic emits in one chunk.
            limit=16 * 1024 * 1024,
        )
        assert self.proc.stdout is not None

        # First line must be {"type": "ready"} — wait for it (with timeout)
        # so a hung worker doesn't block the API request indefinitely.
        try:
            ready_line = await asyncio.wait_for(
                self.proc.stdout.readline(),
                timeout=self.config.claude.timeouts.worker_ready_seconds)
        except asyncio.TimeoutError:
            self.proc.kill()
            raise RuntimeError(
                f"worker for {self.user_id} did not signal ready within "
                f"{self.config.claude.timeouts.worker_ready_seconds:.0f}s")
        if not ready_line:
            raise RuntimeError(
                f"worker for {self.user_id} exited before signalling ready")
        msg = json.loads(ready_line)
        if msg.get("type") != "ready":
            raise RuntimeError(
                f"worker for {self.user_id} sent unexpected first line: {msg}")

        self._reader_task = asyncio.create_task(
            self._read_loop(), name=f"session-reader-{self.user_id}")
        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(), name=f"session-keepalive-{self.user_id}")
        log.info("session up user=%s worker_pid=%s mitm_port=%s",
                 self.user_id, self.proc.pid, self.mitm_port)

    def _seed_home(self, home: Path) -> None:
        """Populate the user's isolated HOME so claude code skips its
        first-run onboarding and shares this worker's assigned account
        credentials.

          - $HOME/.claude.json  -> copied once. claude CLI mutates this
            heavily per session, so each user keeps a private copy to
            avoid concurrent-write races between workers.
          - $HOME/.claude/      -> SYMLINKED (entire directory) to the
            assigned account's directory. Sharing at the directory level
            means a claude-CLI atomic token refresh (write tmp + rename)
            happens *inside* the shared directory — the new file lands
            directly at the source path. All other workers on the same
            account transparently read the rotated token on their next
            call; no copy-back or propagation logic is needed. Per-user
            transcript isolation still works because claude stores
            sessions under cwd-encoded subdirs
            (.claude/projects/<encoded-cwd>/sessions/), and each worker's
            cwd is its own HOME.

        Source-of-truth resolution for the .claude/ symlink target:
          - Multi-account mode: config.account_for_user(user_id).dir
            — the configured `dir:` IS the .claude/ content directory
            (e.g. /data/shared-auth/claude-1 holds .credentials.json
            directly), matching the legacy bind-mount convention where
            /data/shared-auth/claude was mounted to /home/coder/.claude.
          - Legacy single-account mode: operator's ~/.claude.
        Multiple workers belonging to the SAME account safely share the
        directory — same invariant as the original single-account design,
        just scoped per-account: main-process OAuth refresher is the
        sole writer of .credentials.json per account.

        The .claude.json marker (sibling of .claude/, not inside) holds
        the per-account user identity — `oauthAccount` block with email,
        accountUuid, organizationUuid etc., written by `claude /login`.
        claude code 2.1.139+ gates the TUI input prompt on this block:
        without it, even with a valid .credentials.json the status bar
        reports "Not logged in" and a triggered keystroke won't fire a
        /v1/messages call (prewarm then mitm-times-out).

        Source-of-truth resolution for .claude.json (multi-account):
          1. `<account.dir>/.claude.json` — recommended. Lives inside
             the account dir alongside .credentials.json, so a single
             bind mount per account covers everything. claude CLI never
             writes to this path (it writes to per-worker
             `<HOME>/.claude.json`, sibling of `<HOME>/.claude/` — not
             inside it), so the seed file is safely read-only here.
          2. `<account.dir>.json` (sibling file) — legacy split layout.
          3. Operator ~/.claude.json — last-resort fallback (typically
             just the hasCompletedOnboarding stub from entrypoint).
        Legacy single-account mode always uses #3."""
        account = self.config.account_for_user(self.user_id)
        if account is not None:
            src_claude = Path(os.path.expanduser(account.dir)).resolve()
            inside = src_claude / ".claude.json"
            sibling = src_claude.parent / f"{src_claude.name}.json"
            if inside.is_file():
                src_marker = inside
            elif sibling.is_file():
                src_marker = sibling
            else:
                src_marker = Path(os.path.expanduser("~")) / ".claude.json"
        else:
            src_claude = (Path(os.path.expanduser("~")) / ".claude").resolve()
            src_marker = Path(os.path.expanduser("~")) / ".claude.json"

        marker = home / ".claude.json"
        if src_marker.is_file() and not marker.exists():
            shutil.copy2(src_marker, marker)
            log.info("seeded %s/.claude.json from %s", home, src_marker)
        elif not src_marker.is_file() and not marker.exists():
            log.warning(
                "no .claude.json source for user=%s (looked at %s); "
                "claude CLI may show 'Not logged in' and prewarm may fail. "
                "Place the file from your login machine's ~/.claude.json "
                "at the expected path.",
                self.user_id, src_marker)

        claude_dir = home / ".claude"
        if not src_claude.is_dir():
            log.warning(
                "account .claude/ missing at %s (user=%s account=%s); "
                "worker will start in a not-yet-logged-in state",
                src_claude, self.user_id,
                account.dir if account is not None else "<legacy>")
            return

        # Idempotent: leave a correct symlink alone, otherwise rebuild it.
        # The legacy branch handles upgrades from the file-symlink build
        # where a token-refresh rename had replaced the link with a real
        # dir/file holding a now-stale credential.
        if claude_dir.is_symlink():
            try:
                current = Path(os.readlink(claude_dir))
            except OSError:
                current = None
            if current == src_claude:
                return
            claude_dir.unlink()
        elif claude_dir.is_dir():
            log.info("migrating legacy per-user %s into directory symlink",
                     claude_dir)
            shutil.rmtree(claude_dir)
        elif claude_dir.exists():
            claude_dir.unlink()

        claude_dir.symlink_to(src_claude)
        log.info("linked %s -> %s (shared with operator)",
                 claude_dir, src_claude)

    async def stop(self) -> None:
        self._closed = True
        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = None
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
            except ProcessLookupError:
                pass
        if self._reader_task:
            self._reader_task.cancel()
        # Drain any waiting channels so callers don't hang.
        for ch in self._channels.values():
            ch.queue.put_nowait(None)
        self._channels.clear()
        log.info("session stopped user=%s", self.user_id)

    async def call(
        self,
        body: dict[str, Any],
        request_metadata: dict[str, Any] | None = None,
    ) -> ResponseChannel:
        """Submit a /v1/messages body. Returns a channel streaming the
        Anthropic SSE response bytes verbatim. Lock holds until the
        request enters the worker — releasing the lock while the worker
        is still streaming back would let a second caller race with the
        first, since the worker can only serve one model call at a time.

        The proc-alive check is inside the lock so a request arriving
        during a scheduled restart waits for the new worker rather than
        racing with the dead one.

        request_metadata is opaque side-info (e.g. forwarded
        x-litellm-* headers) merged into the channel's body_summary so
        operators can attribute the task on /status without our IPC
        having to know what's in it."""
        async with self.lock:
            return await self._submit(body, request_metadata)

    async def _submit(
        self,
        body: dict[str, Any],
        request_metadata: dict[str, Any] | None = None,
    ) -> ResponseChannel:
        """Lock-free body submission. Caller MUST hold self.lock (or
        guarantee single-writer access some other way). Exists so that
        prewarm flows already running under a restart-held lock can
        submit the dummy bootstrap request without trying to re-enter
        the lock, and without releasing it (which would let a real
        user request race in before bootstrap has populated the
        per-process feature-flag cache)."""
        if self.proc is None or self.proc.returncode is not None:
            raise RuntimeError(f"worker for {self.user_id} not running")
        self.last_used = time.monotonic()
        req_id = self._next_req_id
        self._next_req_id += 1
        channel = ResponseChannel(
            body_summary=_summarize_body(body, request_metadata))
        self._channels[req_id] = channel

        assert self.proc.stdin is not None
        line = json.dumps({"type": "request", "id": req_id,
                           "body": body}) + "\n"
        self.proc.stdin.write(line.encode())
        await self.proc.stdin.drain()
        # DEBUG: per-request and largely redundant with the mitm
        # hijack line. Keep the failure path (worker not running)
        # visible via the exception above.
        log.debug("user=%s submitted req id=%d", self.user_id, req_id)
        return channel

    async def _read_loop(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        try:
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    log.info("worker stdout closed user=%s", self.user_id)
                    return
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("malformed worker stdout: %r", line[:200])
                    continue
                t = msg.get("type")
                # Session-wide events (no req_id) come first; these
                # don't correlate to a specific in-flight channel.
                if t == "rate_limit":
                    # Worker emitted a parsed reset-time event (TUI
                    # modal said e.g. "resets May 27, 12am UTC"). The
                    # absolute epoch lets us mark the account precisely
                    # without falling back to a heuristic window.
                    self._handle_worker_rate_limit_event(msg)
                    continue
                if t == "quota_usage":
                    # Mitm addon captured a /api/oauth/usage response
                    # body (triggered by a /usage probe). Hand the raw
                    # body up to the manager-supplied callback; the
                    # account-name lookup happens there because session
                    # doesn't know which account it belongs to.
                    self._handle_worker_quota_event(msg)
                    continue
                if t == "quota_usage_429":
                    # Upstream rate-limited the probe. Forward with the
                    # parsed retry-after so QuotaProbeService can put
                    # the account into cooldown — saves a wasted /usage
                    # PTY interrupt on every subsequent tick until the
                    # window clears.
                    self._handle_worker_quota_429(msg)
                    continue
                req_id = msg.get("id")
                channel = self._channels.get(req_id)
                if channel is None:
                    continue
                if t == "usage":
                    # Token-usage event emitted by worker AFTER the
                    # channel has been drained but BEFORE the end
                    # marker, so the channel is still in _channels and
                    # its body_summary (carrying the original litellm
                    # user id) is still accessible. We hand off to the
                    # manager callback synchronously — sqlite insert
                    # is fast, no need to spawn a task.
                    self._handle_worker_usage_event(channel, msg)
                    continue
                if t == "chunk":
                    try:
                        data = base64.b64decode(msg.get("data", ""))
                    except Exception:
                        data = b""
                    if data:
                        channel.queue.put_nowait(data)
                        channel.last_chunk_at = time.monotonic()
                        channel.bytes_received += len(data)
                        self._maybe_detect_rate_limit(channel, data)
                elif t == "end":
                    channel.queue.put_nowait(None)
                    self._channels.pop(req_id, None)
                    self._last_channel_close_at = time.monotonic()
                elif t == "error":
                    log.error("user=%s worker error: %s",
                              self.user_id, msg.get("msg"))
                    channel.queue.put_nowait(None)
                    self._channels.pop(req_id, None)
                    self._last_channel_close_at = time.monotonic()
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("reader loop crashed user=%s", self.user_id)
        finally:
            # If worker died, flush every waiting channel. Stamp close
            # time too so pick() doesn't grab the dead session at full
            # priority before respawn paperwork settles.
            for ch in list(self._channels.values()):
                ch.queue.put_nowait(None)
            if self._channels:
                self._last_channel_close_at = time.monotonic()
            self._channels.clear()

    # Roughly 4 KB head buffer is plenty to spot `rate_limit_error`:
    # Anthropic emits the error event as the very first SSE chunk on a
    # rate-limited request, well within this window. Sniffing further
    # is wasted work — once we either detect or pass this threshold we
    # stop scanning.
    _RL_HEAD_LIMIT = 4096
    _RL_MARKER = b"rate_limit_error"
    # Heuristic regex for the human-readable hint in the error message.
    # claude.ai surfaces "weekly", "5-hour" / "5 hour" wording when the
    # respective limit trips. Anything else falls back to a default
    # window that lets the next real request act as the recheck probe.
    _RL_WEEKLY_HINT = re.compile(rb"weekly\s+limit", re.IGNORECASE)
    _RL_5H_HINT = re.compile(rb"(5[\s-]?hour|five[\s-]?hour)\s+limit", re.IGNORECASE)

    def _maybe_detect_rate_limit(self, channel: ResponseChannel,
                                 data: bytes) -> None:
        """If a chunk shows `rate_limit_error`, signal the manager so it
        marks this worker's whole account as rate-limited and routes
        around it until the window expires. Scanning bound: at most
        `_RL_HEAD_LIMIT` bytes per channel, then we give up."""
        if channel._rl_scanned or self._on_rate_limit is None:
            return
        channel._rl_head.extend(data)
        found = self._RL_MARKER in channel._rl_head
        if not found and len(channel._rl_head) < self._RL_HEAD_LIMIT:
            return
        # One-shot: either detected or past the head — release memory.
        channel._rl_scanned = True
        head_bytes = bytes(channel._rl_head)
        channel._rl_head = bytearray()
        if not found:
            return
        # Pick a window from the message text. We err on the SHORT side
        # because pick() will simply skip this account until expiry —
        # marking too long would needlessly black-hole an account that
        # has already recovered. The next real request after expiry
        # serves as a recheck probe, and if still limited gets re-marked.
        # Try every reset-extraction path we know — Anthropic returns
        # reset info in various shapes ("resets MMM DD" text, ISO key,
        # retry-after integer, etc.) and we want the precise time
        # whenever possible.
        try:
            head_text = head_bytes.decode("utf-8", "replace")
        except Exception:
            head_text = ""
        reset_epoch = extract_reset_from_response(head_text)

        # escalate=True asks the manager to apply per-account exponential
        # backoff instead of trusting `window`. Only the bare no-reset
        # generic case below sets it — every authoritative window (parsed
        # reset, weekly, 5-hour) keeps its real reset time.
        escalate = False
        if reset_epoch is not None:
            # Time delta is the authoritative classifier — a body that
            # SAYS "5-hour limit" but RESETS 4 days out is actually the
            # weekly window. Body-text hints (_RL_WEEKLY_HINT / _5H_HINT)
            # are unreliable; size of the window isn't.
            window = max(60.0, reset_epoch - time.time())
            reason = classify_rate_limit_reason(window)
        else:
            # No parseable reset time — fall back to body text hints
            # and a conservative default window. Log the head excerpt
            # so a future failure mode (a wording variant we don't
            # recognise yet) is diagnosable.
            if self._RL_WEEKLY_HINT.search(head_bytes):
                reason, window = "weekly_limit", 3600.0
            elif self._RL_5H_HINT.search(head_bytes):
                reason, window = "5hour_limit", 1800.0
            else:
                # Bare rate_limit_error, no reset header — the rolling
                # TPM/RPM spike. Hand the cooldown to the manager's
                # exponential backoff (window here is just a placeholder).
                reason, window, escalate = "rate_limit", 0.0, True
            excerpt = head_text[:600].replace("\n", " ").replace("\r", " ")
            log.warning(
                "user=%s rate_limit_error detected but no reset time "
                "parsed; reason=%s escalate=%s. Body head excerpt: %r",
                self.user_id, reason, escalate, excerpt)
        try:
            self._on_rate_limit(self.user_id, reason, window, escalate=escalate)
        except Exception:
            log.exception("rate-limit callback failed user=%s", self.user_id)

    def _handle_worker_usage_event(self, channel: ResponseChannel,
                                   msg: dict) -> None:
        """Take the parsed token counts off a worker `usage` IPC line
        and ship them to the manager's UsageStore. The IPC payload
        already has the model + token fields; we add the litellm user
        identifier (from the channel's body_summary) and the worker
        user_id so the store row can be filtered and grouped by all
        three axes without a join.

        The "litellm user" identifier follows what the /ui Worker table
        shows: `user_api_key_alias` first (the virtual-key alias an
        operator configures in LiteLLM and recognises), falling back to
        `user_id` (often a hash or empty) and finally the masked key
        hash. Mismatch with the Worker view confuses operators — same
        request should be labeled the same in both panels."""
        if self._on_usage is None:
            return
        litellm_user = None
        litellm = channel.body_summary.get("litellm") if isinstance(
            channel.body_summary, dict) else None
        if isinstance(litellm, dict):
            # Fallback chain: alias is the human label an operator
            # configured for the virtual key. If that's missing, fall
            # back to the LiteLLM-side user UUID. Only as a last resort
            # do we record the SHA256 of the key itself — it survives
            # everything (LiteLLM always ships it), but it's unreadable
            # noise in the usage table.
            #
            # NB: LiteLLM forwards user identity as x-litellm-user-api-
            # key-user-id, never as a bare x-litellm-user-id. The old
            # fallback's "user_id" key was dead code — it never matched
            # any real LiteLLM header, so any key without an alias
            # tipped straight into hash territory.
            for k in ("user_api_key_alias",
                      "user_api_key_user_id",
                      "user_api_key_user_email",
                      "user_api_key_hash"):
                v = litellm.get(k)
                if isinstance(v, str) and v:
                    litellm_user = v
                    break
        payload = {
            "model": msg.get("model"),
            "input_tokens": int(msg.get("input_tokens") or 0),
            "output_tokens": int(msg.get("output_tokens") or 0),
            "cache_creation_tokens": int(msg.get("cache_creation_tokens") or 0),
            "cache_read_tokens": int(msg.get("cache_read_tokens") or 0),
            "litellm_user": litellm_user,
        }
        try:
            self._on_usage(self.user_id, payload)
        except Exception:
            log.exception("usage event callback failed user=%s",
                          self.user_id)

    def _handle_worker_rate_limit_event(self, msg: dict) -> None:
        """Translate the worker's IPC rate_limit event into a callback
        to the manager. Worker side parses the TUI modal's reset hint
        (precise epoch); we just bound the window defensively and
        invoke the manager's account-level marker."""
        if self._on_rate_limit is None:
            return
        until_epoch = msg.get("until_epoch")
        if not isinstance(until_epoch, (int, float)):
            return
        reason = msg.get("reason") or "rate_limit"
        # Floor at 60s (don't ever mark for less — defends against a
        # parse that yields a stale-past timestamp); cap at 14 days so
        # a buggy parse can't lock out an account indefinitely.
        window = max(60.0, min(float(until_epoch) - time.time(),
                               14 * 86400))
        try:
            # Authoritative reset from the TUI modal — never escalate.
            self._on_rate_limit(self.user_id, reason, window, escalate=False)
        except Exception:
            log.exception("rate-limit event callback failed user=%s",
                          self.user_id)

    def _handle_worker_quota_event(self, msg: dict) -> None:
        """Forward a `/api/oauth/usage` capture to the manager's quota
        callback. We don't validate the body shape here — let
        QuotaProbeService deal with schema drift; it has dedicated logic
        for ignoring junk while keeping the last good snapshot."""
        if self._on_quota is None:
            return
        body = msg.get("body")
        if not isinstance(body, dict):
            return
        fetched_at = msg.get("fetched_at_unix")
        if not isinstance(fetched_at, (int, float)):
            fetched_at = time.time()
        try:
            self._on_quota(self.user_id, body, float(fetched_at))
        except Exception:
            log.exception("quota event callback failed user=%s",
                          self.user_id)

    def _handle_worker_quota_429(self, msg: dict) -> None:
        """Forward a /api/oauth/usage 429 to the manager's 429 hook so
        QuotaProbeService can record a cooldown and stop hammering."""
        if self._on_quota_429 is None:
            return
        retry_after = msg.get("retry_after_seconds")
        if not isinstance(retry_after, (int, float)):
            retry_after = None
        fetched_at = msg.get("fetched_at_unix")
        if not isinstance(fetched_at, (int, float)):
            fetched_at = time.time()
        try:
            self._on_quota_429(self.user_id,
                                float(retry_after) if retry_after is not None else None,
                                float(fetched_at))
        except Exception:
            log.exception("quota 429 callback failed user=%s", self.user_id)

    # How long probe_quota holds the lock past stdin write. Has to cover
    # the worker side's: PTY write /usage → wait for screen render
    # (~1s) → 2s sleep → Esc → screen returns to prompt. 5s gives a
    # comfortable margin; if the dismiss runs faster the lock is just
    # released a moment late — no harm. If a future CLI version slows
    # the screen render, bump this; the symptom would be trigger()
    # racing onto the /usage screen and timing out at 90s with a
    # screen-tail that contains 'Resets MMM DD' text.
    _PROBE_LOCK_HOLD_SECONDS = 5.0

    async def probe_quota(self) -> bool:
        """Send `/usage` to the worker's TUI and hold the lock until
        the worker has had time to dismiss the resulting screen.

        Returns True iff the probe message was written. The actual
        /api/oauth/usage response arrives asynchronously via a
        quota_usage event; this method doesn't wait for it.

        Caller MUST hold self.lock — the lock window enforced here is
        what prevents a concurrent trigger() from typing "say hi" onto
        the /usage screen. The previous (fire-and-forget) design
        released the lock the instant the stdin write returned, well
        before the TUI had finished rendering /usage, which left a
        ~2s race window where a real call could hijack the worker
        mid-probe and time out with /usage text on screen (which the
        timeout fallback misclassified as a rate-limit modal)."""
        if self.proc is None or self.proc.returncode is not None:
            return False
        assert self.proc.stdin is not None
        line = json.dumps({"type": "probe_quota"}) + "\n"
        self.proc.stdin.write(line.encode())
        await self.proc.stdin.drain()
        # Hold the lock while the worker handles the probe end-to-end.
        await asyncio.sleep(self._PROBE_LOCK_HOLD_SECONDS)
        return True

    def idle_seconds(self) -> float:
        return time.monotonic() - self.last_used

    def age_seconds(self) -> float:
        return time.monotonic() - self.started_at

    async def restart(self) -> None:
        """Tear down the current worker subprocess and spin a fresh one
        on the same mitm port. Caller MUST hold self.lock so no new
        request is submitted mid-restart. In-flight SSE streams whose
        bytes were already in transit get truncated (stop() drains their
        channels with a None sentinel)."""
        await self.stop()
        self._closed = False
        self.proc = None
        self._reader_task = None
        self._next_req_id = 0
        self._channels = {}
        self._last_channel_close_at = None
        self.started_at = time.monotonic()
        self.started_at_wall = time.time()
        self.last_used = time.monotonic()
        self.consecutive_intercept_failures = 0
        await self.start()

    # Number of back-to-back intercept failures we tolerate before
    # force-restarting the worker. 2 because a single failure can be
    # the pick→trigger race (which our heuristic already mitigates but
    # cannot prove won't trip once); two in a row makes random transient
    # causes very unlikely and strongly suggests the CLI is wedged.
    _FORCE_RESTART_THRESHOLD = 2

    def mark_intercept_failure(self) -> None:
        """Called by the API handler when a request closed with zero
        bytes received (mitm intercept timeout / watchdog early close).
        Bumps the streak counter; at threshold spawns a background
        force-restart that the next request avoids by virtue of pick()
        seeing the session busy with the restart-holding lock."""
        self.consecutive_intercept_failures += 1
        log.warning("user=%s intercept-failure streak=%d/%d",
                    self.user_id, self.consecutive_intercept_failures,
                    self._FORCE_RESTART_THRESHOLD)
        if (self.consecutive_intercept_failures >= self._FORCE_RESTART_THRESHOLD
                and not self._force_restart_in_progress):
            self._force_restart_in_progress = True
            asyncio.create_task(self._force_restart_async(),
                                 name=f"force-restart-{self.user_id}")

    def mark_request_success(self) -> None:
        """Called by the API handler when bytes flowed on a channel.
        Clears the streak so a worker recovering on its own (single
        transient failure followed by success) doesn't accumulate to
        threshold over hours."""
        if self.consecutive_intercept_failures:
            log.info("user=%s intercept-failure streak cleared (was %d)",
                     self.user_id, self.consecutive_intercept_failures)
            self.consecutive_intercept_failures = 0

    async def _force_restart_async(self) -> None:
        """Spawn-and-forget restart used by mark_intercept_failure().
        Acquires self.lock so any in-flight pick() that landed here
        between the failure and now waits for the new worker."""
        try:
            log.warning("user=%s force-restart: consecutive intercept "
                        "failures hit threshold", self.user_id)
            async with self.lock:
                try:
                    await self.restart()
                    log.info("user=%s force-restart complete", self.user_id)
                except Exception:
                    log.exception("user=%s force-restart failed",
                                  self.user_id)
        finally:
            self._force_restart_in_progress = False
            self.consecutive_intercept_failures = 0

    # How often to self-test an idle worker. 15 min picked so that:
    #   - across 40 workers we average ~2.7 keepalive calls/min, an
    #     imperceptible drop in the quota bucket (haiku, max_tokens=1
    #     → tens of tokens per call)
    #   - a worker that wedges between requests has a bounded "stuck
    #     but undiscovered" window — at most 15 min before keepalive
    #     reveals it (and mark_intercept_failure triggers restart)
    _KEEPALIVE_INTERVAL_SECONDS = 900.0
    # Tiny haiku call. Same shape as the bootstrap prewarm so we know
    # the path works. Cost: ~tens of input tokens + max_tokens=1.
    _KEEPALIVE_BODY: dict[str, Any] = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "ok"}],
    }

    async def _keepalive_loop(self) -> None:
        """Periodic self-test on idle workers. Discovers stuck workers
        (V8 hang, PTY corruption, modal we can't dismiss) BEFORE a
        real user request lands on them — failure here marks the worker
        for force-restart via mark_intercept_failure(), success keeps
        the streak counter clean.

        Strict skip conditions to avoid interfering with real traffic:
          - session closed / worker dead
          - any in-flight channel (worker busy with real request)
          - a force-restart already in flight
          - cannot acquire self.lock within 100ms (someone else won it)
        """
        # Stagger initial sleep across the keepalive interval so 40
        # workers don't all keepalive at the same wall-clock instant
        # (would otherwise produce a thundering-herd quota burst every
        # 15min on the dot).
        try:
            await asyncio.sleep(random.uniform(0, self._KEEPALIVE_INTERVAL_SECONDS))
        except asyncio.CancelledError:
            return
        while not self._closed:
            try:
                await asyncio.sleep(self._KEEPALIVE_INTERVAL_SECONDS)
                if self._closed:
                    return
                if self.proc is None or self.proc.returncode is not None:
                    continue
                if self._channels or self._force_restart_in_progress:
                    continue
                # Try-acquire with tiny timeout: never block a real
                # request waiting on the lock.
                try:
                    await asyncio.wait_for(self.lock.acquire(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                try:
                    try:
                        channel = await self._submit(self._KEEPALIVE_BODY)
                    except Exception:
                        log.exception("user=%s keepalive submit failed",
                                      self.user_id)
                        continue
                    got_bytes = False
                    try:
                        async for chunk in channel.iter():
                            if chunk:
                                got_bytes = True
                    except Exception:
                        log.exception("user=%s keepalive drain error",
                                      self.user_id)
                    if got_bytes:
                        self.mark_request_success()
                        log.debug("user=%s keepalive OK", self.user_id)
                    else:
                        log.warning("user=%s keepalive ping returned 0 bytes "
                                    "— marking intercept failure",
                                    self.user_id)
                        self.mark_intercept_failure()
                finally:
                    if self.lock.locked():
                        self.lock.release()
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("user=%s keepalive loop error", self.user_id)
