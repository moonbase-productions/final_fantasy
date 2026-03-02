from __future__ import annotations
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def _compute_home_field_advantage(df: pd.DataFrame) -> dict:
    """Compute home field advantage (HFA) in Elo points per league.

    HFA is estimated from the historical difference between home and
    away win rates. A higher HFA means the home team wins more often.

    Returns: {league_id -> Elo HFA in points}
    """
    home_games = df.groupby(["league_id", "uid_home"]).size()
    home_wins  = df[df["event_result"] == "home"].groupby(["league_id", "uid_home"]).size()
    home_win_rates = (home_wins / home_games).fillna(0)

    away_games = df.groupby(["league_id", "uid_away"]).size()
    away_wins  = df[df["event_result"] == "away"].groupby(["league_id", "uid_away"]).size()
    away_win_rates = (away_wins / away_games).fillna(0)

    diff = (home_win_rates.groupby("league_id").mean()
            - away_win_rates.groupby("league_id").mean()
            + 0.5)
    diff = diff.clip(0.001, 0.999)  # Avoid log(0)
    home_field_advantage = -400 * np.log10((1 / diff) - 1)
    return home_field_advantage.to_dict()


def _expected_home_win_probability(
    home_elo: float, away_elo: float, home_field_advantage: float
) -> float:
    """Expected win probability for team A (home) vs team B (away)."""
    return 1.0 / (
        1.0 + 10 ** ((away_elo - (home_elo + home_field_advantage)) / 400.0)
    )


def compute_elo_stats(
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute Elo ratings across all events in chronological order.

    All teams start at INIT_ELO (1500). Ratings persist across seasons —
    a team carries its end-of-season rating into the next season.
    Ratings are reset at the start of each run (computed fresh from all history).

    Args:
        events: scored events DataFrame with columns:
            league_id, league_season, league_sport, event_date, event_id,
            uid_home, uid_away, team_score_home, team_score_away, event_result

    Returns:
        Tuple of:
        - summary_df: one row per (uid, league_id, league_season) with Elo stats
        - history_df: one row per (uid, event_id) with per-game Elo data
          including actual_result and expected_win_prob (needed for luck)
    """
    if events.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Initialise ratings for every team seen in the data
    all_uids = pd.concat([events["uid_home"], events["uid_away"]]).unique()
    elo: dict[str, float] = {uid: float(settings.INIT_ELO) for uid in all_uids}

    home_field_advantage_by_league = _compute_home_field_advantage(events)
    history: list[dict] = []

    df_sorted = events.sort_values("event_date").reset_index(drop=True)

    for _, row in df_sorted.iterrows():
        home_uid = row["uid_home"]
        away_uid = row["uid_away"]
        league = row["league_id"]
        sport = row["league_sport"]

        league_home_field_advantage = home_field_advantage_by_league.get(league, 0.0)
        expected_home_result = _expected_home_win_probability(
            elo[home_uid], elo[away_uid], league_home_field_advantage
        )
        expected_away_result = 1.0 - expected_home_result

        k = settings.K_VALUES.get(sport, settings.K_VALUES["default"])

        result = row["event_result"]
        if result == "home":
            actual_h, actual_a = 1.0, 0.0
        elif result == "away":
            actual_h, actual_a = 0.0, 1.0
        else:  # draw
            actual_h, actual_a = 0.5, 0.5

        delta_h = round(k * (actual_h - expected_home_result), 2)
        delta_a = round(k * (actual_a - expected_away_result), 2)

        elo[home_uid] += delta_h
        elo[away_uid] += delta_a

        ts = datetime.now(timezone.utc).isoformat()
        history.append({
            "uid": home_uid,
            "league_id": league,
            "league_season": row["league_season"],
            "event_id": row["event_id"],
            "event_date": row["event_date"],
            "current_elo": round(elo[home_uid], 2),
            "current_elo_delta": delta_h,
            "actual_result": actual_h,
            "expected_win_prob": round(expected_home_result, 4),
        })
        history.append({
            "uid": away_uid,
            "league_id": league,
            "league_season": row["league_season"],
            "event_id": row["event_id"],
            "event_date": row["event_date"],
            "current_elo": round(elo[away_uid], 2),
            "current_elo_delta": delta_a,
            "actual_result": actual_a,
            "expected_win_prob": round(expected_away_result, 4),
        })

    history_df = pd.DataFrame(history).sort_values("event_date")

    # Aggregate to season-level summary
    grp = history_df.groupby(["uid", "league_id", "league_season"])
    summary = grp.agg(
        first_current_elo=("current_elo", "first"),
        first_elo_delta=("current_elo_delta", "first"),
        end_of_season_elo=("current_elo", "last"),
        last_elo_delta=("current_elo_delta", "last"),
    ).reset_index()
    summary["start_of_season_elo"] = (
        summary["first_current_elo"] - summary["first_elo_delta"]
    ).round(2)
    summary["season_elo_delta"] = (
        summary["end_of_season_elo"] - summary["start_of_season_elo"]
    ).round(2)
    summary = summary.drop(columns=["first_current_elo", "first_elo_delta"])

    # League-season rank by end Elo
    summary["start_rank_league"] = summary.groupby(
        ["league_id", "league_season"]
    )["start_of_season_elo"].rank(ascending=False, method="first").fillna(0).astype(int)
    summary["end_rank_league"] = summary.groupby(
        ["league_id", "league_season"]
    )["end_of_season_elo"].rank(ascending=False, method="first").fillna(0).astype(int)

    # Attach per-league HFA so SQL queries can use it for future predictions
    summary["home_field_advantage"] = summary["league_id"].map(
        home_field_advantage_by_league
    ).fillna(0.0).round(2)

    summary["updated_at"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        "compute_elo_stats: %d season rows, %d history rows.",
        len(summary), len(history_df),
    )
    return summary, history_df
