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

# Tool-permission modal anchors. "❯" (U+276F) is the focused-option
# arrow; the digit+dot+label suffixes are unique to claude's permission
# dialog and observed atomic across all TUI redraws we have screen
# dumps for. Module-level so they're constructed once instead of
# re-encoding on every match.
_TOOL_PERMISSION_OPTION1 = "❯1.Yes".encode("utf-8")
_TOOL_PERMISSION_OPTION3 = b"3.No"

# When this byte sequence shows up in the squeezed PTY view, the TUI
# thinks something is in flight on its end ("esc to interrupt" is the
# footer hint claude code renders while a model call streams in or a
# local subprocess runs). If the SessionManager has already picked
# this worker — meaning our _channels is empty — that activity is
# leftover from a previous request that we already finished forwarding;
# typing the placeholder into that state gets the keystroke swallowed
# (mitm never sees /v1/messages). Pre-trigger we send Esc to clear it.
_STALE_INTERRUPT_MARKER = b"esctointerrupt"

# How long after the last drain chunk containing the busy marker we
# still treat the worker as busy. Must exceed the gap between
# consecutive spinner-frame renders (claude CLI redraws the footer
# every ~100-200ms during tool execution) AND cover the gap between
# subagent steps (when one subagent finishes and the next is about to
# print its first marker). 2s matches the existing TUI cooldown that
# pick() applies after channel close, so a session is never picked
# faster than the marker-staleness check anyway.
_BUSY_MARKER_FRESH_SECONDS = 2.0


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
        # Monotonic timestamp of the most recent drain chunk that
        # contained the "esc to interrupt" footer marker. is_tui_busy()
        # treats the worker as busy while this is fresh. The buffer-scan
        # heuristics (screen_tail / _recent_chunks) only catch the
        # marker if it sits in the buffer at lookup time — but when
        # claude CLI is running 6 parallel subagents, the marker is
        # generated faster than the 16KB buffer can preserve it, so
        # individual snapshots will sometimes miss it. A timestamp
        # outlives the byte buffer: any chunk that ever contained the
        # marker keeps the worker marked busy for BUSY_MARKER_FRESH
        # seconds afterward, which spans the gap between spinner
        # frames even under heavy subagent output. Set by
        # `_dismiss_if_dialog` on every chunk (debounce-independent).
        self._last_busy_marker_seen: float = 0.0
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
        # Disable claude code's in-process auto-updater. It runs
        # `npm i -g @anthropic-ai/claude-code` against the global node
        # prefix, which in our container is owned by root and not
        # writable by uid 1000 — so the update always FAILS, but
        # before failing it hangs the TUI for ~30-60s, during which
        # our placeholder keystrokes get swallowed and mitm intercept
        # times out. We pin the CLI version at image build time
        # (Dockerfile ARG CLAUDE_CODE_VERSION); upgrades are a manual
        # rebuild, not a runtime ambush. Env-var documented in claude
        # code source as the canonical disable knob.
        env["DISABLE_AUTOUPDATER"] = "1"
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
        2.1.144 sometimes ignores 1-char submissions).

        Before typing the placeholder, force a synchronous dismiss-scan
        of the current `_recent_chunks` view. Reason: SessionManager
        sees a worker as "idle" the moment its last response channel
        closes (channels pop on `event: message_stop`), but the TUI can
        be lingering on a tool_permission modal that the response just
        produced. Without this pre-trigger dismiss, our placeholder
        keystrokes land INSIDE the modal, the modal eats them, mitm
        never sees a /v1/messages, and the caller hits an
        intercept-timeout 90s later. Forcing the scan here bypasses the
        normal background-drain debounce so the dismiss fires
        immediately rather than waiting for the next chunk to arrive.
        """
        if not self.proc or not self.proc.isalive():
            raise RuntimeError("claude PTY not alive")
        dismissed = self._force_dismiss_before_trigger()
        if dismissed:
            # Give the TUI time to repaint after the dismiss key so our
            # placeholder doesn't land before the modal is gone. 80ms
            # covers a single screen refresh on most setups without
            # adding noticeable latency to the happy path (which won't
            # even enter this branch).
            await asyncio.sleep(0.08)
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

    def _force_dismiss_before_trigger(self) -> bool:
        """Bypass-debounce dismiss scan used by `trigger()` to clear any
        stale modal that survived the last request. Same matching path
        as `_dismiss_if_dialog`, but ignores the debounce window so a
        modal that appeared between requests gets handled even if the
        background drain just dismissed something else.

        Returns True iff a dismiss key was actually written — caller
        uses this to decide whether to wait for the TUI to repaint
        before typing the placeholder."""
        if not self.proc or not self.proc.isalive():
            return False
        try:
            text = bytes(self._recent_chunks).decode("utf-8", "replace")
        except Exception:
            return False
        s = re.sub(r"\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]", "", text)
        s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)
        s = re.sub(r"\x1b[@-Z\\-_]", "", s)
        s_squeezed = re.sub(r"\s+", "", s).encode("utf-8", "replace")
        # Two-layer match: prefer the 4KB chunk window (cheapest, no
        # false positives from screen redraw artifacts), but fall back
        # to the 16KB ANSI-stripped screen_tail. We observed cases where
        # a tool-permission modal sat at the bottom of the rendered
        # screen while claude TUI was *also* spinning agent activity
        # ("Running 2 agents… Read ... Bash command ...") — those redraw
        # chunks flooded the 4KB window and pushed the modal anchors
        # out, so `_matching_rules(s_squeezed)` came back empty even
        # though the modal was still on screen, eating our keystroke.
        # screen_tail is the post-render view and survives that flood.
        candidate_views = [s_squeezed]
        try:
            tail_squeezed = re.sub(r"\s+", "",
                                    self.dump_screen_tail()).encode(
                                        "utf-8", "replace")
            if tail_squeezed and tail_squeezed != s_squeezed:
                candidate_views.append(tail_squeezed)
        except Exception:
            log.debug("pre-trigger screen_tail squeeze failed",
                      exc_info=True)
        for view in candidate_views:
            for _markers, key, name in self._matching_rules(view):
                try:
                    self.proc.write(key)
                    self._last_dialog_dismiss = time.monotonic()
                    self._recent_chunks.clear()
                    log.warning("pre-trigger dismissed claude TUI modal=%s "
                                "key=%r (avoids modal-eats-placeholder race; "
                                "matched via %s view)",
                                name, key,
                                "chunks" if view is s_squeezed else "screen_tail")
                    return True
                except Exception:
                    log.exception("pre-trigger dismiss failed modal=%s", name)
                    return False
        # No known modal matched — check for the generic "stale
        # in-flight" signal. This is the case where a previous response
        # finished from OUR perspective (channel closed, _channels empty,
        # manager handed us a new request) but the TUI is still on a
        # mid-stream screen with the "esc to interrupt" footer hint
        # showing. We observed this with cases:
        #   - Spinner dots + filename + "↓ N tokens" left on screen.
        #   - "Baking…" mid-response with footer still showing interrupt.
        # In both cases the typed placeholder lands into a TUI that
        # isn't reading input → mitm never sees /v1/messages → 90s
        # timeout. One Esc clears the lingering state; the 80ms
        # post-dismiss sleep in trigger() lets the TUI repaint the
        # prompt before we type.
        if _STALE_INTERRUPT_MARKER in s_squeezed:
            try:
                self.proc.write(b"\x1b")
                self._last_dialog_dismiss = time.monotonic()
                self._recent_chunks.clear()
                log.warning("pre-trigger Esc-interrupted stale TUI "
                            "activity (esc-to-interrupt footer present "
                            "but manager picked us as idle)")
                return True
            except Exception:
                log.exception("pre-trigger Esc-interrupt failed")
                return False
        return False

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
        # Esc cancels cleanly: claude CLI does NOT execute the tool,
        # does NOT send a tool_result follow-up (which would burn quota),
        # and returns to IDLE so the next trigger works. Safe because
        # the model's tool_use blocks are for the *API caller* — claude
        # CLI on this worker executing them too is redundant + harmful.
        #
        # Anchor history:
        #   v1 ("Esctocancel", "Tabtoamend") — footer hints; got
        #        fragmented as "Esctocacl" / "Tabtomend" by cursor
        #        positioning, broke matching.
        #   v2 ("Doyouwanttoproceed", "2.Yes,allow") — header + option;
        #        better, but we then observed "Doyouwattoproceed"
        #        (missing 'n') in another failure dump. Still fragile.
        #   v3 (current) "❯1.Yes" + "3.No" — both are 4-6 char atomic
        #        strings that the TUI renders as a single contiguous
        #        run. "❯" is the prompt marker U+276F (3-byte UTF-8),
        #        and the digit+dot+capital combo is unique to this
        #        modal across the CLI surface area we've seen. Both
        #        must match (AND) to keep the false-positive rate at 0.
        ((_TOOL_PERMISSION_OPTION1, _TOOL_PERMISSION_OPTION3),
         b"\x1b", "tool_permission"),
        # Session-feedback survey newer claude-code TUI may pop:
        # "How is Claude doing this session? (optional)
        #  1: Bad  2: Fine  3: Good  0: Dismiss"
        # 0 is the no-side-effect choice. Without this rule the modal
        # eats our trigger keystrokes and every request times out at
        # mitm_intercept_timeout with 0 bytes returned.
        ((b"HowisClaudedoing", b"0:Dismiss"), b"0", "feedback_survey"),
        # /usage slash-command screen ("Session · Total cost · Current
        # session · Resets ... · Current week ..."). Triggered by our
        # quota probe writing "/usage\r" — the HTTP call to
        # /api/oauth/usage fires during the screen render, mitm captures
        # the response, and then we want the TUI back at the prompt
        # ASAP so a subsequent trigger() doesn't type "say hi" INTO this
        # screen (which has 'd to day', 'w to week' shortcuts and the
        # 'Resets May DD' text — the latter previously fooled the
        # mitm-intercept-timeout fallback into marking the account as
        # weekly_limit rate-limited). Markers chosen to be unique to
        # this screen across CLI versions: tab bar text + footer hint.
        ((b"SettingsStatusConfigUsageStats", b"Esctocancel"),
         b"\x1b", "usage_command"),
    )

    def _matching_rules(self, squeezed: bytes) -> list[tuple[tuple[bytes, ...], bytes, str]]:
        return [r for r in self._DISMISS_RULES if all(m in squeezed for m in r[0])]

    def is_tui_busy(self) -> bool:
        """Heuristic: is claude CLI doing something that would swallow
        an inbound placeholder keystroke?

        Returns True for either of:
          - the `esc to interrupt` footer hint is in screen_tail (CLI
            is mid-tool-execution / mid-spinner — the user-facing SSE
            may already have closed, but claude CLI is still running
            the tool chain triggered by tool_use blocks in the response)
          - any known dismiss-rule modal is currently rendered (modal
            grabs input — typed "say hi" gets treated as a 1/2/3 choice)

        Consults BOTH the 16KB screen_tail AND the 4KB _recent_chunks
        view, matching what `_force_dismiss_before_trigger()` checks
        at trigger time. Without the chunks view, pick() and trigger()
        could disagree under load: pick() reads screen_tail at T0 and
        decides idle, then ~50ms later the drain loop appends a fresh
        "esc to interrupt" spinner frame into _recent_chunks, and
        trigger() at T1 has to fire pre-trigger Esc on a worker the
        manager just handed out — exactly the 06:21 / 06:42 burst
        failure pattern. Checking both views in pick() closes that
        race window so the worker gets bypassed entirely.

        Used by SessionManager.pick() so a worker mid-tool-chain isn't
        handed a new user request — the placeholder write would land
        into a TUI that's not reading the prompt line, mitm would never
        see the outbound /v1/messages, and the API caller would get a
        90s upstream_unavailable.

        Best-effort: any decode/regex failure on one view falls through
        to the others; if all views fail the worker is treated as idle
        (a broken heuristic can't permanently exclude a worker)."""
        # Timestamp check first: catches the case where heavy subagent
        # output flushed the marker out of both byte buffers between
        # snapshots. _dismiss_if_dialog updates this on every drain
        # chunk; while the tool chain is rendering spinner frames the
        # timestamp keeps refreshing, so this returns True the entire
        # time the worker is busy. After the chain ends the spinner
        # stops and the timestamp goes stale within
        # _BUSY_MARKER_FRESH_SECONDS.
        if self._last_busy_marker_seen:
            since = time.monotonic() - self._last_busy_marker_seen
            if since < _BUSY_MARKER_FRESH_SECONDS:
                return True
        views: list[bytes] = []
        try:
            tail_squeezed = re.sub(r"\s+", "",
                                    self.dump_screen_tail()).encode(
                                        "utf-8", "replace")
            if tail_squeezed:
                views.append(tail_squeezed)
        except Exception:
            pass
        try:
            text = bytes(self._recent_chunks).decode("utf-8", "replace")
            s = re.sub(r"\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]", "", text)
            s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)
            s = re.sub(r"\x1b[@-Z\\-_]", "", s)
            chunks_squeezed = re.sub(r"\s+", "", s).encode("utf-8", "replace")
            if chunks_squeezed and (not views or chunks_squeezed != views[0]):
                views.append(chunks_squeezed)
        except Exception:
            pass
        for view in views:
            if _STALE_INTERRUPT_MARKER in view:
                return True
            if self._matching_rules(view):
                return True
        return False

    def matching_modal_names(self) -> list[str]:
        """Names of `_DISMISS_RULES` whose markers are all present in
        EITHER the 4KB chunk matcher view OR the 16KB rendered screen
        tail (squeezed). Both views matter: a modal that's been pushed
        out of `_recent_chunks` by mid-tool redraw activity can still
        be sitting at the bottom of the rendered screen, eating input.
        Exposed for the watchdog's diagnostic dump (worker.py) so the
        operator can see which known modals were on screen when a
        request timed out — and, by elimination, when NONE matches,
        the blocking modal is new and needs its own rule added here."""
        names: list[str] = []
        seen: set[str] = set()
        chunks_view = self.dump_recent_chunks_squeezed().encode(
            "utf-8", "replace")
        try:
            tail_view = re.sub(r"\s+", "", self.dump_screen_tail()).encode(
                "utf-8", "replace")
        except Exception:
            tail_view = b""
        for view in (chunks_view, tail_view):
            for _, _, name in self._matching_rules(view):
                if name not in seen:
                    seen.add(name)
                    names.append(name)
        return names

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

        # Strip ANSI + squeeze whitespace for robust matching. Terminal
        # cursor moves can render text without spaces between words
        # (cursor positioning instead of typing space), so we squeeze
        # before substring-matching. Done BEFORE the dismiss-debounce
        # check because the busy-marker timestamp must keep updating
        # even while we're holding off on firing another Esc.
        try:
            text = bytes(self._recent_chunks).decode("utf-8", "replace")
        except Exception:
            return
        s = re.sub(r"\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]", "", text)
        s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)
        s = re.sub(r"\x1b[@-Z\\-_]", "", s)
        s_squeezed = re.sub(r"\s+", "", s).encode("utf-8", "replace")

        now = time.monotonic()
        # Refresh busy-marker timestamp on every drain chunk that
        # contains the footer hint, regardless of debounce. is_tui_busy()
        # consults this so a worker mid-tool-chain stays excluded from
        # pick() even when the marker has scrolled past the byte
        # buffers between snapshots.
        if _STALE_INTERRUPT_MARKER in s_squeezed:
            self._last_busy_marker_seen = now

        if now - self._last_dialog_dismiss < self._dialog_dismiss_debounce:
            return  # debounce — shared across dismiss rules
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
