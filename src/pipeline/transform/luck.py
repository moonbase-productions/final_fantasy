from __future__ import annotations
import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def compute_luck(elo_history: pd.DataFrame) -> pd.DataFrame:
    """Compute Elo-adjusted luck score for each team.

    Definition:
        luck_raw = mean(actual_result) - mean(expected_win_prob)
                   over the last LUCK_WINDOW games

    Positive = winning more than Elo predicted (lucky).
    Negative = winning less than Elo predicted (unlucky).

    luck_display: 0-100 integer, percentile rank of luck_raw across all teams.
    Used directly as the 'asset_luck' display value on the website.

    Args:
        elo_history: output from compute_elo_stats, one row per (uid, event_id)
            Must have columns: uid, league_id, event_date,
            actual_result, expected_win_prob

    Returns:
        DataFrame with columns: uid, league_id, luck_score, luck_display
    """
    if elo_history.empty:
        return pd.DataFrame(columns=["uid", "league_id", "luck_score", "luck_display"])

    # Take the most recent LUCK_WINDOW games per team
    recent = (
        elo_history
        .sort_values("event_date", ascending=False)
        .groupby("uid", group_keys=False)
        .head(settings.LUCK_WINDOW)
    )

    luck = (
        recent
        .groupby(["uid", "league_id"])
        .apply(
            lambda g: g["actual_result"].mean() - g["expected_win_prob"].mean()
        )
        .reset_index(name="luck_score")
    )

    luck["luck_display"] = (
        luck["luck_score"]
        .rank(pct=True)
        .mul(100)
        .round()
        .astype(int)
    )

    logger.info("compute_luck: %d team luck scores computed.", len(luck))
    return luck
