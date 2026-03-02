"""tests/test_transform/test_normalize.py."""
from __future__ import annotations

from pipeline.transform.normalize import (
    _safe_int,
    decompose_race_events,
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


def test_normalize_events_multi_competitor_binary_fallback() -> None:
    """Multi-competitor events in home/away format normalize like binary."""
    events = [
        {"league_id": "300", "team_score_home": 25.0, "team_score_away": 18.0},
        {"league_id": "300", "team_score_home": 10.0, "team_score_away": 15.0},
    ]
    out = normalize_events(events, {"300": "multi_competitor"})
    assert len(out) == 2
    # Higher score wins -> 1.0/0.0
    assert out[0]["team_score_home"] == 1.0
    assert out[0]["team_score_away"] == 0.0
    # Lower home score -> 0.0/1.0
    assert out[1]["team_score_home"] == 0.0
    assert out[1]["team_score_away"] == 1.0


def test_normalize_events_multi_competitor_with_finish_position() -> None:
    """Multi-competitor events with finish_position get decomposed."""
    events = [
        {"event_id": "race1", "league_id": "300", "league_sport": "Motorsport",
         "league_season": "2024", "uid": "300-A", "finish_position": 1,
         "event_date": "2024-03-01", "event_status": "Match Finished",
         "updated_at": "2024-03-01T00:00:00"},
        {"event_id": "race1", "league_id": "300", "league_sport": "Motorsport",
         "league_season": "2024", "uid": "300-B", "finish_position": 2,
         "event_date": "2024-03-01", "event_status": "Match Finished",
         "updated_at": "2024-03-01T00:00:00"},
        {"event_id": "race1", "league_id": "300", "league_sport": "Motorsport",
         "league_season": "2024", "uid": "300-C", "finish_position": 3,
         "event_date": "2024-03-01", "event_status": "Match Finished",
         "updated_at": "2024-03-01T00:00:00"},
    ]
    out = normalize_events(events, {"300": "multi_competitor"})
    # 3 drivers -> 3*(3-1)/2 = 3 pairwise events
    assert len(out) == 3
    # Each event should have uid_home/uid_away
    for e in out:
        assert "uid_home" in e
        assert "uid_away" in e
        assert "team_score_home" in e


def test_decompose_race_events_pairwise_count() -> None:
    """N drivers produce N*(N-1)/2 pairwise events."""
    from pipeline.config import settings
    results = [
        {"event_id": "r1", "league_id": "300", "league_season": "2024",
         "event_date": "2024-03-01", "uid": f"300-{i}",
         "finish_position": i, "league_sport": "Motorsport",
         "event_status": "Match Finished", "updated_at": "2024-03-01"}
        for i in range(1, 6)  # 5 drivers
    ]
    pairwise = decompose_race_events(results, settings.F1_POINTS)
    # 5 * 4 / 2 = 10
    assert len(pairwise) == 10


def test_decompose_race_events_scores_are_points() -> None:
    """Higher finisher gets more points in the pairwise event."""
    from pipeline.config import settings
    results = [
        {"event_id": "r1", "league_id": "300", "league_season": "2024",
         "event_date": "2024-03-01", "uid": "300-A",
         "finish_position": 1, "league_sport": "Motorsport",
         "event_status": "Match Finished", "updated_at": "2024-03-01"},
        {"event_id": "r1", "league_id": "300", "league_season": "2024",
         "event_date": "2024-03-01", "uid": "300-B",
         "finish_position": 2, "league_sport": "Motorsport",
         "event_status": "Match Finished", "updated_at": "2024-03-01"},
    ]
    pairwise = decompose_race_events(results, settings.F1_POINTS)
    assert len(pairwise) == 1
    event = pairwise[0]
    assert event["uid_home"] == "300-A"  # P1
    assert event["uid_away"] == "300-B"  # P2
    assert event["team_score_home"] == 25  # F1 P1 points
    assert event["team_score_away"] == 18  # F1 P2 points


def test_decompose_race_events_empty_input() -> None:
    """Empty input returns empty list."""
    assert decompose_race_events([], {}) == []


def test_safe_int_normal_values() -> None:
    assert _safe_int(1) == 1
    assert _safe_int(3.0) == 3
    assert _safe_int("5") == 5


def test_safe_int_with_none() -> None:
    assert _safe_int(None) == 99
    assert _safe_int(None, 50) == 50


def test_safe_int_with_nan() -> None:
    assert _safe_int(float("nan")) == 99
    assert _safe_int(float("nan"), 42) == 42


def test_safe_int_with_invalid() -> None:
    assert _safe_int("abc") == 99


def test_decompose_race_events_nan_finish_position() -> None:
    """Race result with NaN finish_position uses default (99), doesn't crash."""
    from pipeline.config import settings
    results = [
        {"event_id": "r1", "league_id": "300", "league_season": "2024",
         "event_date": "2024-03-01", "uid": "300-A",
         "finish_position": 1, "league_sport": "Motorsport",
         "event_status": "Match Finished", "updated_at": "2024-03-01"},
        {"event_id": "r1", "league_id": "300", "league_season": "2024",
         "event_date": "2024-03-01", "uid": "300-B",
         "finish_position": None, "league_sport": "Motorsport",
         "event_status": "Match Finished", "updated_at": "2024-03-01"},
    ]
    pairwise = decompose_race_events(results, settings.F1_POINTS)
    assert len(pairwise) == 1
    # P1 (position 1) gets 25 pts, None position gets 0 pts (position 99 not in map)
    assert pairwise[0]["team_score_home"] == 25
    assert pairwise[0]["team_score_away"] == 0.0
