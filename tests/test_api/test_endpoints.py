"""tests/test_api/test_endpoints.py"""
from __future__ import annotations

from pipeline.api.endpoints import (
    THESPORTSDB_API_BASE_URL,
    all_leagues_url,
    league_detail_url,
    seasons_url,
    teams_url,
    season_events_url,
)


def test_base_url_constant() -> None:
    assert THESPORTSDB_API_BASE_URL == "https://www.thesportsdb.com/api/v2/json"


def test_all_leagues_url() -> None:
    assert all_leagues_url() == f"{THESPORTSDB_API_BASE_URL}/all/leagues"


def test_lookup_urls() -> None:
    assert league_detail_url(4328) == f"{THESPORTSDB_API_BASE_URL}/lookup/league/4328"
    assert seasons_url("4328") == f"{THESPORTSDB_API_BASE_URL}/list/seasons/4328"
    assert teams_url(4328) == f"{THESPORTSDB_API_BASE_URL}/list/teams/4328"


def test_season_events_url() -> None:
    assert (
        season_events_url(4328, "2024-2025")
        == f"{THESPORTSDB_API_BASE_URL}/filter/events/4328/2024-2025"
    )
