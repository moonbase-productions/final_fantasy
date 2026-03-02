"""tests/test_runners/test_onboard.py — tests for league onboarding."""
from __future__ import annotations

from pipeline.runners.onboard import detect_new_leagues, onboard_leagues


class _FakeExecuteResult:
    def __init__(self, data: list[dict]):
        self.data = data


class _FakeTable:
    def __init__(self, data: list[dict]):
        self._data = data
        self._filters: dict[str, object] = {}

    def select(self, *_a, **_kw):
        return self

    def eq(self, field, value):
        self._filters[field] = value
        return self

    def is_(self, field, value):
        self._filters[f"is_{field}"] = value
        return self

    def execute(self):
        return _FakeExecuteResult(self._data)


class _FakeSchema:
    def __init__(self, tables: dict[str, list[dict]]):
        self._tables = tables

    def table(self, name: str):
        return _FakeTable(self._tables.get(name, []))


class _FakeSupabase:
    def __init__(self, schemas: dict[str, dict[str, list[dict]]]):
        self._schemas = schemas

    def schema(self, name: str):
        return _FakeSchema(self._schemas.get(name, {}))


def test_detect_new_leagues_returns_never_fetched() -> None:
    """Leagues with is_active=True and last_fetched_at=NULL are detected."""
    fake = _FakeSupabase({
        "admin": {
            "league_registry": [
                {"league_id": "100", "sport_type": "standard", "league_name": "Test League"},
            ],
        },
    })
    result = detect_new_leagues(fake)
    assert len(result) == 1
    assert result[0]["league_id"] == "100"


def test_detect_new_leagues_empty_when_none() -> None:
    """No never-fetched leagues returns empty list."""
    fake = _FakeSupabase({"admin": {"league_registry": []}})
    result = detect_new_leagues(fake)
    assert result == []


def test_onboard_leagues_noop_on_empty_list() -> None:
    """Empty new_leagues list does nothing (no exceptions)."""
    onboard_leagues(None, None, None, [], {})
