"""tests/test_runners/test_scheduler.py — tests for auto-activation scheduler."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.runners.scheduler import auto_activate_leagues


class _FakeExecuteResult:
    def __init__(self, data: list[dict]):
        self.data = data


class _FakeTable:
    def __init__(self, data: list[dict]):
        self._data = data

    def select(self, *_a, **_kw):
        return self

    def eq(self, field, value):
        return self

    @property
    def not_(self):
        return self

    def is_(self, field, value):
        return self

    def in_(self, field, values):
        return self

    def gte(self, field, value):
        return self

    def lte(self, field, value):
        return self

    def limit(self, n):
        return self

    def update(self, data):
        self._last_update = data
        return self

    def execute(self):
        return _FakeExecuteResult(self._data)


class _FakeSchema:
    def __init__(self, tables: dict[str, _FakeTable]):
        self._tables = tables

    def table(self, name: str):
        return self._tables.get(name, _FakeTable([]))


class _FakeSupabase:
    def __init__(self, schemas: dict[str, dict[str, _FakeTable]]):
        self._schemas = schemas

    def schema(self, name: str):
        return _FakeSchema(self._schemas.get(name, {}))


def test_auto_activate_finds_upcoming_events() -> None:
    """Leagues with upcoming events get activated."""
    tomorrow = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()

    registry_table = _FakeTable([
        {"league_id": "500", "league_name": "Test League", "sport_type": "standard"},
    ])
    events_table = _FakeTable([
        {"league_id": "500"},
    ])

    fake = _FakeSupabase({
        "admin": {"league_registry": registry_table},
        "api": {"events": events_table},
    })

    result = auto_activate_leagues(fake, lookahead_days=14)
    assert len(result) == 1
    assert result[0]["league_id"] == "500"
    assert result[0]["league_name"] == "Test League"


def test_auto_activate_skips_no_events() -> None:
    """Leagues without upcoming events are not activated."""
    registry_table = _FakeTable([
        {"league_id": "600", "league_name": "Off-season League", "sport_type": "standard"},
    ])
    events_table = _FakeTable([])  # No upcoming events

    fake = _FakeSupabase({
        "admin": {"league_registry": registry_table},
        "api": {"events": events_table},
    })

    result = auto_activate_leagues(fake, lookahead_days=14)
    assert result == []


def test_auto_activate_no_candidates() -> None:
    """No whitelisted-but-inactive leagues returns empty."""
    registry_table = _FakeTable([])  # No candidates

    fake = _FakeSupabase({
        "admin": {"league_registry": registry_table},
    })

    result = auto_activate_leagues(fake, lookahead_days=14)
    assert result == []
