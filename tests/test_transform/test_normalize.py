"""tests/test_transform/test_normalize.py."""
from __future__ import annotations

from pipeline.transform.normalize import (
    normalize_events,
    sanitize_date,
    sanitize_score,
    sanitize_time,
)


def test_sanitize_date_handles_zero_values() -> None:
    assert sanitize_date("0000-00-00") == "1970-01-01"
    assert sanitize_date("2024-00-15") == "2024-01-15"
    assert sanitize_date("") == "1970-01-01"


def test_sanitize_time_normalizes_suffixes() -> None:
    assert sanitize_time("18:30:00 ET") == "18:30:00"
    assert sanitize_time("18:30:00 PM ET") == "18:30:00"
    assert sanitize_time("18:30:00:00") == "18:30:00"
    assert sanitize_time("") == "12:00:00"


def test_sanitize_score_casts_or_returns_none() -> None:
    assert sanitize_score("3") == 3.0
    assert sanitize_score(2) == 2.0
    assert sanitize_score("") is None
    assert sanitize_score(None) is None
    assert sanitize_score("null") is None
    assert sanitize_score("abc") is None


def test_normalize_events_binary_conversion() -> None:
    events = [
        {"league_id": "100", "team_score_home": 2.0, "team_score_away": 1.0},
        {"league_id": "100", "team_score_home": 1.0, "team_score_away": 1.0},
        {"league_id": "100", "team_score_home": None, "team_score_away": None},
    ]
    out = normalize_events(events, {"100": "binary"})
    assert out[0]["team_score_home"] == 1.0
    assert out[0]["team_score_away"] == 0.0
    assert out[1]["team_score_home"] == 0.5
    assert out[1]["team_score_away"] == 0.5
    assert out[2]["team_score_home"] is None
    assert out[2]["team_score_away"] is None


def test_normalize_events_standard_passthrough() -> None:
    events = [{"league_id": "200", "team_score_home": 3.0, "team_score_away": 2.0}]
    out = normalize_events(events, {"200": "standard"})
    assert out == events
