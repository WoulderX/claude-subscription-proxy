"""Parser for claude CLI's rate-limit reset time hints.

Two surfaces emit a reset timestamp:

  1. The TUI modal shown when an account has hit its weekly/5h quota.
     Format example (post-ANSI-strip):
         You've hit your limit · resets May 27, 12am (UTC)

  2. The /v1/messages 429 error body (`rate_limit_error.message`), which
     may include an ISO-8601 reset stamp or the same human-readable form.

Both flow through this module so the proxy records the EXACT moment to
clear the account-level block, instead of guessing a conservative window.
"""
from __future__ import annotations

import calendar
import datetime as dt
import re

# {"january": 1, "february": 2, ...} — calendar.month_name[0] is "" so
# we filter it out. Lowercased for case-insensitive match.
_MONTH_NAMES = {name.lower(): i
                for i, name in enumerate(calendar.month_name) if name}
# claude CLI sometimes abbreviates ("May" stays "May", but "September"
# might come through as "Sep" on narrow terminal widths). Add 3-letter
# abbrevs.
_MONTH_ABBRS = {name.lower(): i
                for i, name in enumerate(calendar.month_abbr) if name}
_MONTHS = {**_MONTH_NAMES, **_MONTH_ABBRS}

# Match "resets May 27, 12am (UTC)" and reasonable variants. All
# whitespace runs use `\s*` (zero-or-more) rather than `\s+` so the
# regex also matches the PTY matcher's whitespace-squeezed view
# ("resetsMay27,12am(UTC)") — claude code's TUI redraws strip a lot
# of whitespace when our chunk buffer ingests them, and we'd rather
# match leniently than miss the cue. The trailing month/digit anchors
# keep this from matching random text.
_RESET_HUMAN_RE = re.compile(
    r"resets?\s*(?:at\s*)?"
    r"([A-Z][a-z]{2,})\s*(\d{1,2}),?\s*"   # Month Day,
    r"(\d{1,2})(?:\s*:\s*(\d{2}))?\s*"     # Hour[:minute]
    r"(am|pm)?\s*"                          # am/pm (optional, 24h if absent)
    r"\(?\s*(UTC|GMT)?\s*\)?",              # (UTC) (optional)
    re.IGNORECASE,
)

# Match an ISO-8601 timestamp that the API may embed in error messages:
# "resets at 2026-05-27T05:00:00Z" or "...at 2026-05-27T05:00:00+00:00".
_RESET_ISO_RE = re.compile(
    r"resets?\s*(?:at\s*)?"
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:Z|[+-]\d{2}:?\d{2})?)",
    re.IGNORECASE,
)

# Bare ISO-8601 — used when we already know we're inside a
# rate_limit_error context, so any ISO timestamp nearby is almost
# certainly the reset moment. Matches Anthropic's likely JSON shapes
# without requiring an exact key match:
#   "resets_at": "2026-05-27T05:00:00Z"
#   "reset_at": "2026-05-27T05:00:00Z"
#   "x-ratelimit-reset": "2026-05-27T05:00:00Z"
_BARE_ISO_RE = re.compile(
    r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?"
    r"(?:Z|[+-]\d{2}:?\d{2})?)\b"
)

# Match retry-after style fields (HTTP header or JSON key). Value is
# seconds from "now". Anthropic and most rate-limited APIs use this
# rather than an absolute timestamp.
_RETRY_AFTER_RE = re.compile(
    rb"(?:retry[_-]?after|"
    rb"x[_-]?retry[_-]?after|"
    rb"anthropic[_-]ratelimit[_-]\w*?[_-]reset|"
    rb"x[_-]?ratelimit[_-]reset)"
    rb"[\"':\s,]+(\d{1,10})",
    re.IGNORECASE,
)

# Anthropic's unified rate-limit headers (e.g. anthropic-ratelimit-
# unified-5h-reset) carry an ABSOLUTE epoch in seconds. Distinguish
# from a relative retry-after by value range: anything above 1.5e9
# (≈2017+) is clearly an absolute epoch, not a relative duration.
_ABSOLUTE_EPOCH_THRESHOLD = 1_500_000_000


def classify_rate_limit_reason(seconds_to_reset: float) -> str:
    """Pick the reason label from the size of the reset window, NOT
    from the header / body text. Anthropic's 429 wording is sometimes
    misleading — a body that mentions "5-hour limit" can actually
    carry a reset 4 days out (the operator hit the 7-day limit while
    text still references the daily window). The reset delta is the
    ground truth: a 5-hour limit can never reset more than ~5h in
    the future, so anything beyond that is a longer-cycle limit.

      < 6h    → "5hour_limit"   (rolling 5-hour message window)
      6h–36h  → "rate_limit"    (rare middle ground; unknown semantics)
      > 36h   → "weekly_limit"  (7-day usage window — Pro/Max plans)

    We leave a small buffer beyond 5h (6h cutoff) so a 5h reset
    measured slightly before the boundary still classifies right."""
    if seconds_to_reset > 36 * 3600:
        return "weekly_limit"
    if seconds_to_reset > 6 * 3600:
        return "rate_limit"
    return "5hour_limit"


def parse_reset_time(text: str, *, now: dt.datetime | None = None) -> float | None:
    """Find a reset timestamp in `text` and return it as Unix epoch (UTC).
    Returns None if no recognisable form is found or the matched values
    don't make sense.

    `now` is exposed for tests so we can assert year-rollover behaviour
    deterministically; production callers leave it None and we read the
    current UTC time."""
    if not text:
        return None

    iso = _try_iso(text)
    if iso is not None:
        return iso

    return _try_human(text, now=now)


def extract_reset_from_response(text: str, *, now: float | None = None) -> float | None:
    """More aggressive variant of `parse_reset_time` for use AFTER
    we've confirmed the response body contains `rate_limit_error`.
    The context lets us trust any nearby timestamp / retry-after as
    "the reset", instead of requiring an explicit "resets" prefix.

    Order of attempts:
      1. parse_reset_time (explicit "resets" prefix or anchored ISO)
      2. Bare ISO timestamp anywhere in the buffer
      3. retry-after / x-ratelimit-reset numeric value — absolute
         epoch if > ~2017, otherwise treated as seconds-from-now."""
    if not text:
        return None

    via_human = parse_reset_time(text)
    if via_human is not None:
        return via_human

    # Bare ISO — first match wins. parse_reset_time's ISO regex requires
    # a "resets" prefix; this one doesn't, useful when the API returns
    # a structured JSON like `{"resets_at":"..."}` where our prefix
    # regex would miss it.
    m = _BARE_ISO_RE.search(text)
    if m:
        raw = m.group(1)
        parsed_raw = raw[:-1] if raw.endswith("Z") else raw
        try:
            parsed = dt.datetime.fromisoformat(parsed_raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.timestamp()
        except ValueError:
            pass

    # retry-after / *-reset header or JSON field. Value is either an
    # absolute epoch (seconds since 1970) or a relative duration in
    # seconds. Distinguish by magnitude.
    m = _RETRY_AFTER_RE.search(text.encode("utf-8", "replace"))
    if m:
        try:
            v = int(m.group(1))
        except ValueError:
            return None
        if v >= _ABSOLUTE_EPOCH_THRESHOLD:
            return float(v)
        # Relative: anchored at "now". Caller can pass a value (mostly
        # for tests); default is wall-clock.
        import time as _t
        base = _t.time() if now is None else float(now)
        return base + float(v)

    return None


def _try_iso(text: str) -> float | None:
    m = _RESET_ISO_RE.search(text)
    if not m:
        return None
    raw = m.group(1)
    # Python <3.11 dt.datetime.fromisoformat doesn't accept the trailing
    # 'Z'; strip it and assume UTC ourselves. 3.11+ accepts both.
    parsed_raw = raw[:-1] if raw.endswith("Z") else raw
    try:
        parsed = dt.datetime.fromisoformat(parsed_raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _try_human(text: str, *, now: dt.datetime | None) -> float | None:
    m = _RESET_HUMAN_RE.search(text)
    if not m:
        return None
    month_name, day_s, hour_s, minute_s, ampm, _tz = m.groups()

    month = _MONTHS.get(month_name.lower())
    if month is None:
        return None

    try:
        day = int(day_s)
        hour = int(hour_s)
        minute = int(minute_s) if minute_s else 0
    except ValueError:
        return None

    if ampm:
        # 12-hour clock: 12am = hour 0 (midnight), 12pm = hour 12 (noon).
        hour = hour % 12
        if ampm.lower() == "pm":
            hour += 12

    if not (0 <= hour < 24 and 0 <= minute < 60 and 1 <= day <= 31):
        return None

    if now is None:
        now = dt.datetime.now(dt.timezone.utc)

    try:
        target = dt.datetime(year=now.year, month=month, day=day,
                             hour=hour, minute=minute, second=0,
                             tzinfo=dt.timezone.utc)
    except ValueError:
        # e.g. Feb 30 — bogus, give up.
        return None

    # The TUI omits the year; if the parsed date has already passed,
    # the reset must be in the next calendar year (boundary case around
    # Dec → Jan).
    if target <= now:
        try:
            target = target.replace(year=now.year + 1)
        except ValueError:
            return None

    return target.timestamp()
