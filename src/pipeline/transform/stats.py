from __future__ import annotations
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def compute_basic_stats(events: pd.DataFrame) -> pd.DataFrame:
    """Compute win/loss/draw and points stats for every team per season.

    Input DataFrame must have columns:
        league_id, league_season, league_sport, event_date,
        uid_home, uid_away, team_score_home, team_score_away, event_result
        (event_result: 'home' | 'away' | 'draw')

    Returns one row per (uid, league_id, league_season) with:
        wins, draws, losses, points_for, points_against, games_played,
        avg_points_for, avg_points_against, win_percentage,
        home_wins, home_draws, home_losses, home_points_for,
        home_points_against, home_games_played,
        avg_home_points_for, avg_home_points_against, home_win_percentage,
        avg_points_for_percentile, avg_points_against_percentile
    """
    if events.empty:
        logger.warning("compute_basic_stats called with empty DataFrame.")
        return pd.DataFrame(columns=[
            "uid", "league_id", "league_season",
            "wins", "draws", "losses", "points_for", "points_against", "games_played",
            "avg_points_for", "avg_points_against", "win_percentage",
            "home_wins", "home_draws", "home_losses", "home_points_for",
            "home_points_against", "home_games_played",
            "avg_home_points_for", "avg_home_points_against", "home_win_percentage",
            "avg_points_for_percentile", "avg_points_against_percentile",
        ])

    # --- Build long-format DataFrame ---
    # Home perspective
    home = events[
        ["event_id", "league_id", "league_season", "event_date",
         "uid_home", "team_score_home", "team_score_away", "event_result"]
    ].copy()
    home.columns = [
        "event_id", "league_id", "league_season", "event_date",
        "uid", "pf", "pa", "event_result",
    ]
    home["is_home"] = True
    home["win"]  = (home["event_result"] == "home").astype(int)
    home["draw"] = (home["event_result"] == "draw").astype(int)
    home["loss"] = (home["event_result"] == "away").astype(int)

    # Away perspective
    away = events[
        ["event_id", "league_id", "league_season", "event_date",
         "uid_away", "team_score_away", "team_score_home", "event_result"]
    ].copy()
    away.columns = [
        "event_id", "league_id", "league_season", "event_date",
        "uid", "pf", "pa", "event_result",
    ]
    away["is_home"] = False
    away["win"]  = (away["event_result"] == "away").astype(int)
    away["draw"] = (away["event_result"] == "draw").astype(int)
    away["loss"] = (away["event_result"] == "home").astype(int)

    long = pd.concat([home, away], ignore_index=True)

    # --- Aggregate: all games ---
    grp = long.groupby(["uid", "league_id", "league_season"])
    stats = grp.agg(
        wins=("win", "sum"),
        draws=("draw", "sum"),
        losses=("loss", "sum"),
        points_for=("pf", "sum"),
        points_against=("pa", "sum"),
        games_played=("win", "count"),
    ).reset_index()

    # --- Aggregate: home games only ---
    home_only = long[long["is_home"]].groupby(["uid", "league_id", "league_season"])
    home_stats = home_only.agg(
        home_wins=("win", "sum"),
        home_draws=("draw", "sum"),
        home_losses=("loss", "sum"),
        home_points_for=("pf", "sum"),
        home_points_against=("pa", "sum"),
        home_games_played=("win", "count"),
    ).reset_index()

    stats = stats.merge(home_stats, on=["uid", "league_id", "league_season"], how="left")

    # --- Derived columns ---
    stats["avg_points_for"] = stats["points_for"] / stats["games_played"].clip(lower=1)
    stats["avg_points_against"] = stats["points_against"] / stats["games_played"].clip(lower=1)
    stats["win_percentage"] = (
        (stats["wins"] + 0.5 * stats["draws"]) / stats["games_played"].clip(lower=1)
    ).round(4)
    stats["avg_home_points_for"] = (
        stats["home_points_for"] / stats["home_games_played"].clip(lower=1)
    )
    stats["avg_home_points_against"] = (
        stats["home_points_against"] / stats["home_games_played"].clip(lower=1)
    )
    stats["home_win_percentage"] = (
        (stats["home_wins"] + 0.5 * stats["home_draws"])
        / stats["home_games_played"].clip(lower=1)
    ).round(4)

    # --- Percentile ranks within each league-season ---
    stats["avg_points_for_percentile"] = stats.groupby(
        ["league_id", "league_season"]
    )["avg_points_for"].rank(pct=True, ascending=False)
    stats["avg_points_against_percentile"] = stats.groupby(
        ["league_id", "league_season"]
    )["avg_points_against"].rank(pct=True, ascending=True)

    logger.info("compute_basic_stats: produced %d rows.", len(stats))
    return stats
