"""Unit tests for the rate-limit reset-time parser. Targets:

  - The TUI modal wording claude CLI shows on a hit account
    ("You've hit your limit · resets May 27, 12am (UTC)")
  - ISO timestamps that may appear in /v1/messages 429 bodies
  - Year-rollover edge case (parsed date in the past → next year)
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.rate_limit import parse_reset_time


_REF = dt.datetime(2026, 5, 23, 18, 0, 0, tzinfo=dt.timezone.utc)


def test_tui_modal_text():
    """The exact wording the user reported from the TUI modal."""
    text = "You've hit your limit · resets May 27, 12am (UTC)"
    epoch = parse_reset_time(text, now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed == dt.datetime(2026, 5, 27, 0, 0, 0, tzinfo=dt.timezone.utc)


def test_tui_modal_pm():
    text = "...resets June 3, 11pm (UTC)..."
    epoch = parse_reset_time(text, now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed.hour == 23 and parsed.month == 6 and parsed.day == 3


def test_tui_modal_noon_vs_midnight():
    midnight = parse_reset_time("resets May 27, 12am (UTC)", now=_REF)
    noon = parse_reset_time("resets May 27, 12pm (UTC)", now=_REF)
    assert midnight is not None and noon is not None
    a = dt.datetime.fromtimestamp(midnight, tz=dt.timezone.utc)
    b = dt.datetime.fromtimestamp(noon, tz=dt.timezone.utc)
    assert a.hour == 0
    assert b.hour == 12


def test_with_minutes():
    text = "resets May 27, 5:30am UTC"
    epoch = parse_reset_time(text, now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed.hour == 5 and parsed.minute == 30


def test_year_rollover():
    """A reset date that's BEFORE now must roll over to next year."""
    # now = May 23 2026; "resets Jan 5, 12am" must be Jan 5 2027.
    epoch = parse_reset_time("resets Jan 5, 12am (UTC)", now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed.year == 2027 and parsed.month == 1 and parsed.day == 5


def test_iso_timestamp():
    text = 'rate_limit_error: ...resets at 2026-05-27T05:00:00Z please retry'
    epoch = parse_reset_time(text, now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed == dt.datetime(2026, 5, 27, 5, 0, 0, tzinfo=dt.timezone.utc)


def test_iso_with_offset():
    text = "resets 2026-05-27T08:30:00+00:00"
    epoch = parse_reset_time(text, now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed.hour == 8 and parsed.minute == 30


def test_month_abbreviation():
    epoch = parse_reset_time("resets Jun 3, 12am UTC", now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed.month == 6 and parsed.day == 3


def test_no_match_returns_none():
    assert parse_reset_time("just some random text") is None
    assert parse_reset_time("") is None
    assert parse_reset_time("resets soon") is None


def test_bogus_date_rejected():
    # Feb 30 doesn't exist
    assert parse_reset_time("resets Feb 30, 12am UTC", now=_REF) is None


def test_real_log_excerpt():
    """Realistic excerpt with TUI noise around the marker. Parser should
    still pick it out — that's the whole point of regex-based scan."""
    text = """
    ╭─── Claude Code v2.1.139 ─────────╮
    │ Welcome back!
    │ ...
    │ You've hit your limit · resets May 27, 12am (UTC)
    │ ...
    ╰──────────────────────────────────╯
    ? for shortcuts | Not logged in
    """
    epoch = parse_reset_time(text, now=_REF)
    assert epoch is not None
    parsed = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    assert parsed.month == 5 and parsed.day == 27 and parsed.hour == 0
