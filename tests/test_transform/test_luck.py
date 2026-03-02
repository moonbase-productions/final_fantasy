"""tests/test_transform/test_luck.py"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.luck import compute_luck


def _make_history(uid: str, results: list[tuple[float, float]]) -> list[dict]:
    """results: list of (actual_result, expected_win_prob) tuples."""
    return [
        {
            "uid":               uid,
            "league_id":         "100",
            "event_id":          f"e{i}",
            "event_date":        f"2024-01-{i+1:02d}",
            "actual_result":     a,
            "expected_win_prob": e,
        }
        for i, (a, e) in enumerate(results)
    ]


def test_returns_dataframe():
    history = pd.DataFrame(
        _make_history("100-1", [(1.0, 0.5)] * 5)
    )
    result = compute_luck(history)
    assert isinstance(result, pd.DataFrame)


def test_required_columns():
    history = pd.DataFrame(_make_history("100-1", [(1.0, 0.5)] * 5))
    result = compute_luck(history)
    assert {"uid", "league_id", "luck_score", "luck_display"}.issubset(result.columns)


def test_positive_luck_when_outperforming():
    """Always winning when expected 50/50 → positive luck_score."""
    history = pd.DataFrame(
        _make_history("100-1", [(1.0, 0.5)] * 10)
        + _make_history("100-2", [(0.5, 0.5)] * 10)
    )
    result = compute_luck(history)
    t1 = result[result["uid"] == "100-1"].iloc[0]
    t2 = result[result["uid"] == "100-2"].iloc[0]
    assert t1["luck_score"] > t2["luck_score"]


def test_luck_display_in_range():
    """luck_display should always be in [0, 100]."""
    history = pd.DataFrame(
        _make_history("100-1", [(1.0, 0.9)] * 10)
        + _make_history("100-2", [(0.0, 0.1)] * 10)
        + _make_history("100-3", [(0.5, 0.5)] * 10)
    )
    result = compute_luck(history)
    assert (result["luck_display"] >= 0).all()
    assert (result["luck_display"] <= 100).all()


def test_empty_input():
    result = compute_luck(pd.DataFrame())
    assert result.empty
