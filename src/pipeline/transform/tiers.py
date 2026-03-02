from __future__ import annotations
import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def compute_tiers(elo_summary: pd.DataFrame) -> list[dict]:
    """Assign tier labels to teams based on end-of-season Elo percentile.

    Tiers are assigned globally across ALL leagues and sports in one pass.
    This means a B-tier team in Soccer and a B-tier team in Basketball have
    comparable Elo percentile standing globally, not just within their sport.

    Uses TIER_THRESHOLDS from settings. Tiers in descending order:
    MOL (top 0.5%), SS, S, A, B, C, D, E, F, FF, DIE (bottom 0.5%)

    Takes the most recent season's Elo for each team (highest season).

    Returns list of dicts for upsert into py_tier.
    """
    if elo_summary.empty:
        return []

    # Get the most recent season per team
    latest = (
        elo_summary
        .sort_values("league_season", ascending=False)
        .groupby("uid")
        .first()
        .reset_index()
    )

    latest["percentile_rank"] = latest["end_of_season_elo"].rank(pct=True)
    now = datetime.now(timezone.utc).isoformat()

    def assign_tier(pct: float) -> str:
        for threshold, tier_name in settings.TIER_THRESHOLDS:
            if pct > threshold:
                return tier_name
        return "DIE"

    latest["tier"] = latest["percentile_rank"].apply(assign_tier)
    latest["updated_at"] = now

    return latest[["uid", "league_id", "tier", "updated_at"]].to_dict(orient="records")
