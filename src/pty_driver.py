from __future__ import annotations

import asyncio
import logging
import os
import re
import select
import time
from pathlib import Path

import ptyprocess

log = logging.getLogger(__name__)

# Bytes that appear in claude code's TUI once it is ready to accept input.
# "❯ " is the prompt cursor; the placeholder hint "Try" comes alongside it.
# We wait for either to appear before sending input.
_READY_MARKERS = (b"\xe2\x9d\xaf", b"Try ")
_READY_TIMEOUT = 15.0


class ClaudePtyDriver:
    """Spawns `claude` in a PTY with HTTPS_PROXY pointing at our mitm port.
    Provides `trigger()` to submit a placeholder prompt that causes claude
    code to emit one outbound /v1/messages — its content will be replaced
    by the mitm addon before it leaves the loopback.

    We deliberately drop all TUI output (no scraping) — the model's real
    response is read directly off the network via mitm."""

    def __init__(self, binary: str, home: Path, https_proxy: str, ca_cert: Path) -> None:
        self.binary = binary
        self.home = home
        self.https_proxy = https_proxy
        self.ca_cert = ca_cert
        self.proc: ptyprocess.PtyProcess | None = None
        self._drain_task: asyncio.Task | None = None
        # Ring buffer of recent PTY output bytes. When mitm intercept
        # times out (claude TUI received our trigger keystroke but never
        # actually sent a /v1/messages), we dump the tail of this buffer
        # so an operator can see WHAT the TUI was showing on screen.
        # 16 KB so that a thinking-spinner animation (which can spam
        # several KB of redraws per second) doesn't push the actual
        # status text — the part we care about — out of the window.
        self._screen_tail: bytearray = bytearray()
        self._screen_tail_limit: int = 16384
        # Debounce timestamp for auto-dismiss of claude CLI's tool
        # permission dialog. The drain loop sees ~10 chunks/sec and the
        # dialog's footer text persists in several consecutive chunks
        # until Esc takes effect; this keeps us from sending Esc 20+
        # times in a row for one dialog. Set short (500ms) so that
        # cascades of dialogs (CC sub-agent with 30+ tool_use blocks)
        # get cleared in seconds, not minutes — claude CLI itself takes
        # ~100ms to process each Esc.
        self._last_dialog_dismiss: float = 0.0
        self._dialog_dismiss_debounce: float = 0.5
        # A small sliding buffer that holds the last ~4KB of chunks so
        # we can detect dialog markers even when they're split across
        # multiple os.read() calls. Cleared on every successful dismiss
        # so old dialog text doesn't keep triggering matches after the
        # dialog is gone.
        self._recent_chunks: bytearray = bytearray()
        self._recent_chunks_limit: int = 4096
        # Rate-limit modal capture. claude TUI may briefly show
        # "You've hit your limit · resets MMM DD, hham UTC" on startup
        # and then redraw over it within seconds — by the time mitm-
        # intercept times out (90s) the rolling screen-tail buffer has
        # long lost it. So we scan EVERY chunk for "resets" and save a
        # KB-sized snippet around the first match for the worker to
        # query later. Saved once and frozen — we don't want a later
        # screen redraw clobbering our captured evidence.
        self._rate_limit_snippet: bytes = b""

    async def start(self) -> None:
        env = os.environ.copy()
        env["HOME"] = str(self.home)
        env["HTTPS_PROXY"] = self.https_proxy
        env["HTTP_PROXY"] = self.https_proxy
        env["NODE_EXTRA_CA_CERTS"] = str(self.ca_cert)
        # Strip env vars that mark THIS process as a claude-code child; we
        # want the new subprocess to look like an independent CLI launch.
        for k in ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID",
                  "CLAUDE_CODE_ENTRYPOINT", "AI_AGENT"):
            env.pop(k, None)

        # Spawn synchronously — ptyprocess.PtyProcess.spawn is fast and
        # going through run_in_executor leaves the PTY fd associated with
        # the worker thread's handler in a way that subsequent writes
        # from the main asyncio thread are silently swallowed.
        self.proc = ptyprocess.PtyProcess.spawn(
            [self.binary],
            env=env,
            cwd=str(self.home),
            dimensions=(40, 120),
        )
        loop = asyncio.get_running_loop()
        # Wait until the TUI reaches the input prompt before we start the
        # background drain task. Claude code 2.1.144 takes 5-8s on a cold
        # boot to fetch bootstrap + MCP registry + paint the prompt.
        await loop.run_in_executor(None, self._wait_until_ready)
        self._drain_task = asyncio.create_task(self._drain())

    def _wait_until_ready(self) -> None:
        import time
        assert self.proc is not None
        fd = self.proc.fd
        seen = b""
        ready_at: float | None = None
        deadline = time.monotonic() + _READY_TIMEOUT
        # Two-phase: (1) wait until prompt marker appears, then (2) keep
        # draining for an additional 2.5s so Ink finishes its mount cycle.
        # Without (2), keystrokes sent immediately after the prompt paints
        # tend to be lost.
        while time.monotonic() < deadline:
            r, _, _ = select.select([fd], [], [], 0.2)
            if r:
                try:
                    chunk = os.read(fd, 8192)
                except OSError:
                    break
                if chunk:
                    seen += chunk
            if ready_at is None and any(m in seen for m in _READY_MARKERS):
                ready_at = time.monotonic()
                log.info("pty TUI prompt visible after %d bytes; draining "
                         "for 2.5s to let Ink bind input handlers",
                         len(seen))
            if ready_at is not None and time.monotonic() - ready_at >= 2.5:
                log.info("pty TUI ready, total boot bytes=%d", len(seen))
                return
        log.warning("pty TUI not ready within %.1fs; sending input anyway",
                    _READY_TIMEOUT)

    async def stop(self) -> None:
        if self._drain_task:
            self._drain_task.cancel()
        if self.proc and self.proc.isalive():
            try:
                self.proc.terminate(force=True)
            except Exception:
                pass

    async def send_slash_command(self, cmd: str) -> None:
        """Send a TUI slash command (e.g. `/usage`) to claude. Slash
        commands are handled INSIDE the TUI — they don't go through the
        normal placeholder-then-Enter pipeline that `trigger()` uses for
        /v1/messages. The TUI dispatches the command to its own handler
        (which may or may not make HTTP calls); we don't observe the
        result here — capture happens via the mitm addon when the TUI's
        outbound HTTP request hits the proxy."""
        if not self.proc or not self.proc.isalive():
            raise RuntimeError("claude PTY not alive")
        data = (cmd + "\r").encode()
        self.proc.write(data)
        log.debug("pty slash-command wrote %d bytes: %r", len(data), data)

    async def trigger(self, placeholder: str = "say hi") -> None:
        """Type a placeholder + submit so claude emits one outbound
        request. The placeholder is thrown away — mitm swaps the real body.
        Multi-char strings are more reliable than single chars (claude code
        2.1.144 sometimes ignores 1-char submissions)."""
        if not self.proc or not self.proc.isalive():
            raise RuntimeError("claude PTY not alive")
        # Use ptyprocess.write which goes through its buffered file
        # object + flush — equivalent to what the probe_trigger.py probe
        # did successfully.
        data = (placeholder + "\r").encode()
        self.proc.write(data)
        # DEBUG: per-request. trigger never fails silently — the
        # caller observes mitm-intercept-timeout if claude TUI
        # swallowed the keystroke, and that path logs at WARNING.
        log.debug("pty trigger wrote %d bytes via ptyprocess: %r",
                  len(data), data)

    # Modal auto-dismiss rules. Each entry is (markers, key, name):
    #   markers — all of these byte substrings must appear in the
    #             ANSI-stripped + whitespace-squeezed `_recent_chunks`
    #             view for the rule to fire. Pick anchors that are
    #             unique to one modal and stable across CLI versions /
    #             terminal widths (e.g. the · in footer hints is U+00B7,
    #             NOT a whitespace, so it survives squeeze).
    #   key     — bytes to write back into the PTY to dismiss.
    #   name    — short label used in log lines and exposed via
    #             `matching_modal_names()` for diagnostic dumps.
    #
    # Rules are tried in order; only the first matching one fires per
    # debounce window so cascading dismisses don't fight each other.
    _DISMISS_RULES: tuple[tuple[tuple[bytes, ...], bytes, str], ...] = (
        # Tool-permission dialog ("1. Yes  2. Yes always  3. No").
        # Footer is "Esc to cancel · Tab to amend · ctrl+e to explain".
        # Esc cancels cleanly: claude CLI does NOT execute the tool,
        # does NOT send a tool_result follow-up (which would burn quota),
        # and returns to IDLE so the next trigger works. Safe because
        # the model's tool_use blocks are for the *API caller* — claude
        # CLI on this worker executing them too is redundant + harmful.
        ((b"Esctocancel", b"Tabtoamend"), b"\x1b", "tool_permission"),
        # Session-feedback survey newer claude-code TUI may pop:
        # "How is Claude doing this session? (optional)
        #  1: Bad  2: Fine  3: Good  0: Dismiss"
        # 0 is the no-side-effect choice. Without this rule the modal
        # eats our trigger keystrokes and every request times out at
        # mitm_intercept_timeout with 0 bytes returned.
        ((b"HowisClaudedoing", b"0:Dismiss"), b"0", "feedback_survey"),
    )

    def _matching_rules(self, squeezed: bytes) -> list[tuple[tuple[bytes, ...], bytes, str]]:
        return [r for r in self._DISMISS_RULES if all(m in squeezed for m in r[0])]

    def matching_modal_names(self) -> list[str]:
        """Names of `_DISMISS_RULES` whose markers are all present in
        the current `_recent_chunks` matcher view. Exposed for the
        watchdog's diagnostic dump (worker.py) so an operator can see
        which known modals were on screen when a request timed out —
        and, by elimination, when NONE matches, that the modal blocking
        input is new and needs its own rule added here."""
        return [name for _, _, name in self._matching_rules(
            self.dump_recent_chunks_squeezed().encode("utf-8", "replace"))]

    def _dismiss_if_dialog(self, chunk: bytes) -> None:
        """Apply `_DISMISS_RULES` against recent PTY output. Without
        this, modals like the tool-permission dialog or the session-
        feedback survey eat the `"say hi\\r"` keystroke that `trigger()`
        sends, mitm never sees a /v1/messages, the request times out at
        mitm_intercept_timeout with 0 bytes, and the modal persists for
        the next attempt too.

        Detection uses a small sliding `_recent_chunks` buffer instead
        of just this chunk — a modal often renders across two or three
        os.read() calls as the TUI repaints, so the footer hint and the
        header text may not arrive together. After a successful dismiss
        the buffer is cleared so the old text doesn't keep matching."""
        if not self.proc or not self.proc.isalive():
            return
        # Accumulate into the sliding buffer first so we cover dialogs
        # that span multiple chunks. Trim from the left to keep the
        # buffer bounded; we only need enough to hold one dialog frame.
        self._recent_chunks.extend(chunk)
        if len(self._recent_chunks) > self._recent_chunks_limit:
            del self._recent_chunks[:-self._recent_chunks_limit]

        now = time.monotonic()
        if now - self._last_dialog_dismiss < self._dialog_dismiss_debounce:
            return  # debounce — shared across all rules

        # Strip ANSI + squeeze whitespace for robust matching. Terminal
        # cursor moves can render text without spaces between words
        # (cursor positioning instead of typing space), so we squeeze
        # before substring-matching.
        try:
            text = bytes(self._recent_chunks).decode("utf-8", "replace")
        except Exception:
            return
        s = re.sub(r"\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]", "", text)
        s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)
        s = re.sub(r"\x1b[@-Z\\-_]", "", s)
        s_squeezed = re.sub(r"\s+", "", s).encode("utf-8", "replace")
        for _markers, key, name in self._matching_rules(s_squeezed):
            try:
                self.proc.write(key)
                self._last_dialog_dismiss = now
                self._recent_chunks.clear()
                log.warning("auto-dismissed claude TUI modal=%s key=%r",
                            name, key)
            except Exception:
                log.exception("failed to dismiss modal=%s", name)
            return  # one dismiss per debounce window

    # Anchor that appears in claude TUI's rate-limit modal. We require
    # both "resets" AND a month name (Jan-Dec) within a small window
    # so we don't fire on unrelated occurrences like "reset settings"
    # or "rest" hyphenated across a line break.
    _RL_SCAN = re.compile(
        rb"resets?\s*(?:at\s*)?"
        rb"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        rb"[a-z]{0,8}\s*\d{1,2}",
        re.IGNORECASE,
    )

    def _capture_rate_limit_text(self, chunk: bytes) -> None:
        """Cheap per-chunk scan for the rate-limit modal text. Once
        captured the snippet is FROZEN — a later screen redraw won't
        clobber it. Uses `_recent_chunks` as the search window so
        markers split across multiple os.read() calls still match
        (same trick as _dismiss_if_dialog)."""
        if self._rate_limit_snippet:
            return
        m = self._RL_SCAN.search(self._recent_chunks)
        if m is None:
            return
        # Save ±512 bytes of context around the match. parse_reset_time
        # only needs the immediate vicinity, but extra context helps
        # diagnostics ("what was on screen when we tagged the account?").
        lo = max(0, m.start() - 512)
        hi = min(len(self._recent_chunks), m.end() + 512)
        self._rate_limit_snippet = bytes(self._recent_chunks[lo:hi])
        log.info("captured rate-limit modal snippet (%d bytes) for later parse",
                 len(self._rate_limit_snippet))

    def dump_rate_limit_snippet(self) -> str:
        """Return the captured rate-limit text after the same ANSI-strip
        pipeline screen_tail uses. Empty string if we never saw a
        plausible marker."""
        if not self._rate_limit_snippet:
            return ""
        text = self._rate_limit_snippet.decode("utf-8", "replace")
        s = re.sub(r"\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]", "", text)
        s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)
        s = re.sub(r"\x1b[@-Z\\-_]", "", s)
        return s

    def dump_recent_chunks_squeezed(self) -> str:
        """Return _recent_chunks after the exact same ANSI-strip +
        whitespace-squeeze pipeline that _dismiss_if_dialog uses for
        matching. Lets a caller log "what the dialog matcher was
        actually looking at" alongside the human-readable screen_tail,
        for diagnosing why a dialog wasn't dismissed."""
        try:
            text = bytes(self._recent_chunks).decode("utf-8", "replace")
        except Exception:
            return "(decode failed)"
        s = re.sub(r"\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]", "", text)
        s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)
        s = re.sub(r"\x1b[@-Z\\-_]", "", s)
        return re.sub(r"\s+", "", s)

    def dump_screen_tail(self) -> str:
        """Return the last ~4KB of TUI output as plain text, ANSI escape
        codes stripped, for crash diagnosis. Called when a layer above
        detects something wrong (e.g. mitm intercept timeout) and wants
        to know WHAT the TUI was showing.

        Strips:
          - CSI sequences (\\x1b[...A-Za-z) used for cursor moves / colors
          - OSC sequences (\\x1b]...\\x07 or \\x1b\\) used for window title
          - other 2-byte ESC sequences (\\x1bX where X is a single char)
        """
        raw = bytes(self._screen_tail).decode("utf-8", "replace")
        # CSI: ESC [ params... final-byte
        s = re.sub(r'\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]', '', raw)
        # OSC: ESC ] ... (terminated by BEL or ESC \)
        s = re.sub(r'\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)', '', s)
        # Two-byte ESC sequences (ESC + one char), drop both
        s = re.sub(r'\x1b[@-Z\\-_]', '', s)
        return s

    async def _drain(self) -> None:
        """Continuously read+discard TUI output so the PTY buffer doesn't
        fill up and block the child. Uses select+os.read (non-blocking
        polling) so we never tie up an executor thread waiting on a
        blocking ptyprocess.read()."""
        assert self.proc is not None
        fd = self.proc.fd
        loop = asyncio.get_running_loop()
        while True:
            try:
                # Yield to the loop, then drain whatever is ready.
                await asyncio.sleep(0.1)
                r, _, _ = select.select([fd], [], [], 0)
                while r:
                    try:
                        data = os.read(fd, 8192)
                    except BlockingIOError:
                        break
                    except OSError:
                        return
                    if not data:
                        return
                    # Snapshot into the ring buffer for crash diagnosis.
                    # Cheap: append + trim. Don't decode here — keep
                    # raw bytes including ANSI escapes; dump_screen_tail
                    # strips them on read.
                    self._screen_tail.extend(data)
                    if len(self._screen_tail) > self._screen_tail_limit:
                        del self._screen_tail[:-self._screen_tail_limit]
                    # Auto-dismiss permission dialogs so they don't eat
                    # our PTY trigger keystrokes (see _dismiss_if_dialog
                    # for rationale).
                    self._dismiss_if_dialog(data)
                    # And piggy-back: scan for the rate-limit modal so
                    # we can surface the official reset time on /ui
                    # even when the modal flashes briefly and is then
                    # redrawn out of the rolling buffer.
                    self._capture_rate_limit_text(data)
                    r, _, _ = select.select([fd], [], [], 0)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("pty drain error")
                return
