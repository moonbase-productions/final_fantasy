from __future__ import annotations
import logging
import re
from collections import defaultdict
from typing import Any, Optional

import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def _safe_int(val: object, default: int = 99) -> int:
    """Convert to int, returning *default* for None/NaN/invalid."""
    if val is None:
        return default
    try:
        v = float(val)
        if v != v:  # NaN check
            return default
        return int(v)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Raw value sanitization
# ---------------------------------------------------------------------------

def sanitize_date(date_str: str) -> str:
    """Fix malformed dates from TheSportsDB.

    '0000-00-00' -> '1970-01-01'
    '2024-00-15' -> '2024-01-15'
    """
    if not date_str:
        return "1970-01-01"
    date_str = date_str.replace("0000-00-00", "1970-01-01")
    date_str = re.sub(r"-00", "-01", date_str)
    return date_str


def sanitize_time(time_str: str) -> str:
    """Normalize time strings from TheSportsDB.

    Strips timezone suffixes like ' ET', ' AM ET'.
    Truncates malformed times like '18:30:00:00' to '18:30:00'.
    Returns '12:00:00' for null/empty input.
    """
    if not time_str:
        return "12:00:00"
    time_str = time_str.replace(" AM ET", "").replace(" PM ET", "").replace(" ET", "")
    match = re.match(r"^(\d{2}:\d{2}:\d{2})", time_str)
    if match:
        return match.group(1)
    return time_str or "12:00:00"


def sanitize_score(score_val: Any) -> Optional[float]:
    """Parse a score value to float. Returns None for null/empty/invalid."""
    if score_val is None or score_val == "" or score_val == "null":
        return None
    try:
        return float(score_val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Points maps for multi-competitor sports
# ---------------------------------------------------------------------------

def _get_points_map(league_sport: str) -> dict[int, float]:
    """Return the championship points map for a multi-competitor sport."""
    sport_lower = (league_sport or "").lower()
    if "nascar" in sport_lower:
        return settings.NASCAR_POINTS
    # Default: F1-style points (also used for Formula E, F2, UCI)
    return settings.F1_POINTS


# ---------------------------------------------------------------------------
# Sport-type-specific event normalization
# ---------------------------------------------------------------------------

def normalize_events(
    events: list[dict],
    sport_type_map: dict[str | int, str],
) -> list[dict]:
    """Normalize raw events by sport type before upsert.

    For 'standard' sports: no change — scores already in home/away format.
    For 'binary' sports: replace scores with 1.0 (win) / 0.0 (loss) / 0.5 (draw).
    For 'multi_competitor' sports: if events have finish_position data, group
        by race and decompose into pairwise matchups. Otherwise, normalize like
        binary (winner/loser).

    Args:
        events: list of raw event dicts (from extract layer)
        sport_type_map: {league_id -> sport_type} mapping from league_registry

    Returns:
        Normalized event list. Multi-competitor leagues may produce more rows.
    """
    normalized: list[dict] = []
    # Collect multi-competitor events for batch processing
    multi_competitor_events: list[dict] = []

    for event in events:
        league_id = event.get("league_id")
        sport_type = sport_type_map.get(league_id) or sport_type_map.get(
            int(league_id) if league_id else None
        )

        if sport_type is None:
            logger.warning(
                "Unknown sport_type for league_id=%s; treating as standard.", league_id
            )

        if sport_type == "binary":
            normalized.extend(_normalize_binary(event))
        elif sport_type == "multi_competitor":
            multi_competitor_events.append(event)
        else:
            # standard (or unknown): pass through unchanged
            normalized.append(event)

    # Process multi-competitor events as a batch
    if multi_competitor_events:
        normalized.extend(_normalize_multi_competitor(multi_competitor_events))

    return normalized


def _normalize_binary(event: dict) -> list[dict]:
    """Convert a binary-outcome event (UFC, Tennis, Boxing) to 1.0/0.0 scores.

    If scores are already null, leaves them null (unscored future event).
    If scores exist, replaces with:
      - Winner: 1.0
      - Loser:  0.0
      - Draw/no-contest: both 0.5
    """
    home_score = event.get("team_score_home")
    away_score = event.get("team_score_away")

    if home_score is None and away_score is None:
        # Future event — return as-is
        return [event]

    try:
        h = float(home_score) if home_score is not None else 0
        a = float(away_score) if away_score is not None else 0
    except (TypeError, ValueError):
        return [event]

    if h > a:
        norm_h, norm_a = 1.0, 0.0
    elif a > h:
        norm_h, norm_a = 0.0, 1.0
    else:
        norm_h, norm_a = 0.5, 0.5

    return [{**event, "team_score_home": norm_h, "team_score_away": norm_a}]


def _normalize_multi_competitor(events: list[dict]) -> list[dict]:
    """Normalize multi-competitor events (F1, NASCAR, cycling).

    Two modes depending on the data shape:
    1. Events with finish_position: group by race event_id, decompose into
       pairwise matchups via decompose_race_events().
    2. Events in home/away format (from API head-to-head pairings): normalize
       like binary (winner/loser with 1.0/0.0 scores).
    """
    # Check if any events have finish_position data (race result format)
    race_format = [e for e in events if e.get("finish_position") is not None]
    pairwise_format = [e for e in events if e.get("finish_position") is None]

    result: list[dict] = []

    # Process race-format events: group by base event_id and decompose
    if race_format:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for event in race_format:
            grouped[event["event_id"]].append(event)

        for event_id, group in grouped.items():
            if len(group) < 2:
                logger.warning(
                    "Multi-competitor event %s has only %d result(s); skipping decomposition.",
                    event_id, len(group),
                )
                continue
            sport = group[0].get("league_sport", "")
            points_map = _get_points_map(sport)
            pairwise = decompose_race_events(group, points_map)
            result.extend(pairwise)

        logger.info(
            "Decomposed %d race results into %d pairwise events.",
            len(race_format), len(result),
        )

    # Process home/away format events: normalize like binary
    if pairwise_format:
        for event in pairwise_format:
            result.extend(_normalize_binary(event))
        logger.info(
            "Normalized %d multi-competitor events as binary (home/away format).",
            len(pairwise_format),
        )

    return result


def decompose_race_events(
    race_results: list[dict],
    points_map: dict[int, float],
) -> list[dict]:
    """Convert a list of race results into pairwise matchup events.

    Used for F1, F2, Formula E, NASCAR, UCI Cycling.

    Each race produces N*(N-1)/2 synthetic pairwise events. The higher
    finisher is assigned as uid_home with score = their points, the lower
    finisher as uid_away with score = their points.

    Args:
        race_results: list of dicts, each with:
            {event_id, league_id, league_season, event_date, uid, finish_position,
             league_sport, event_status, updated_at}
        points_map: {finish_position -> championship_points}

    Returns:
        List of synthetic event dicts in standard home/away format.
    """
    pairwise: list[dict] = []
    if not race_results:
        return pairwise

    # Sort by finish position ascending so race_results[i] always beats race_results[j]
    race_results = sorted(race_results, key=lambda r: _safe_int(r.get("finish_position"), 99))

    base = race_results[0]  # Use first result for shared metadata
    n = len(race_results)

    for i in range(n):
        for j in range(i + 1, n):
            r1 = race_results[i]  # higher finisher (lower position number)
            r2 = race_results[j]  # lower finisher (higher position number)

            pos1 = r1.get("finish_position", 99)
            pos2 = r2.get("finish_position", 99)

            pts1 = points_map.get(_safe_int(pos1, 99), 0.0)
            pts2 = points_map.get(_safe_int(pos2, 99), 0.0)

            pairwise.append({
                "event_id": f"{base['event_id']}-{r1['uid']}-{r2['uid']}",
                "event_date": base.get("event_date"),
                "event_time": base.get("event_time", "12:00:00"),
                "league_id": base.get("league_id"),
                "league_sport": base.get("league_sport"),
                "league_season": base.get("league_season"),
                "league_round": base.get("league_round", ""),
                "uid_home": r1["uid"],   # higher finisher = "home"
                "uid_away": r2["uid"],   # lower finisher = "away"
                "team_score_home": pts1,
                "team_score_away": pts2,
                "event_status": base.get("event_status", "Match Finished"),
                "event_video": "",
                "updated_at": base.get("updated_at"),
            })

    return pairwise
