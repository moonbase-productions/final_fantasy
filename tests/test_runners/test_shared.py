"""tests/test_runners/test_shared.py — tests for shared runner utilities."""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.runners.shared import DERIVED_SQL_FILES, compute_and_prepare_stats


def test_derived_sql_files_count() -> None:
    """All 17 SQL query files should be listed."""
    assert len(DERIVED_SQL_FILES) == 17


def test_derived_sql_files_ordering() -> None:
    """SQL files should be in ascending numeric order."""
    nums = [int(f.split("_")[0]) for f in DERIVED_SQL_FILES]
    assert nums == sorted(nums)
    assert nums == list(range(1, 18))


def test_compute_and_prepare_stats_empty() -> None:
    """Empty DataFrame returns empty lists without error."""
    stats, tiers = compute_and_prepare_stats(pd.DataFrame())
    assert stats == []
    assert tiers == []


def test_compute_and_prepare_stats_produces_records(minimal_events: pd.DataFrame) -> None:
    """Given scored events, returns non-empty stats and tier records."""
    stats, tiers = compute_and_prepare_stats(minimal_events)
    assert len(stats) > 0
    assert len(tiers) > 0
    # Each stat record should have uid and league_season
    for rec in stats:
        assert "uid" in rec
        assert "league_season" in rec


def test_compute_and_prepare_stats_int_coercion(minimal_events: pd.DataFrame) -> None:
    """INT columns should be actual Python ints, not floats."""
    stats, _ = compute_and_prepare_stats(minimal_events)
    int_cols = {"wins", "losses", "draws", "games_played", "luck_display"}
    for rec in stats:
        for col in int_cols:
            val = rec.get(col)
            if val is not None:
                assert isinstance(val, int), f"{col} should be int, got {type(val)}"
