"""tests/test_extract/test_events.py"""
from __future__ import annotations

from dataclasses import dataclass

from pipeline.extract.events import _is_season_complete


@dataclass
class _Resp:
    data: object = None
    count: int | None = None


class _RPC:
    def __init__(self, supabase: "_FakeSupabase"):
        self.supabase = supabase

    def execute(self) -> _Resp:
        if self.supabase.rpc_error:
            raise RuntimeError("rpc unavailable")
        return _Resp(data=self.supabase.rpc_data)


class _Query:
    def __init__(self, supabase: "_FakeSupabase") -> None:
        self.supabase = supabase
        self._pending_mode = False

    def select(self, *_, **__) -> "_Query":
        return self

    def eq(self, *_, **__) -> "_Query":
        return self

    def is_(self, *_, **__) -> "_Query":
        self._pending_mode = True
        return self

    def lt(self, *_, **__) -> "_Query":
        return self

    def execute(self) -> _Resp:
        if self._pending_mode:
            return _Resp(data=[{"event_id": "1"}], count=self.supabase.pending_count)
        return _Resp(data=[{"event_id": "1"}], count=self.supabase.total_count)


class _FakeSupabase:
    def __init__(
        self,
        *,
        rpc_data: object = None,
        rpc_error: bool = False,
        pending_count: int = 0,
        total_count: int = 1,
    ) -> None:
        self.rpc_data = rpc_data
        self.rpc_error = rpc_error
        self.pending_count = pending_count
        self.total_count = total_count

    def rpc(self, *_args, **_kwargs) -> _RPC:
        return _RPC(self)

    def schema(self, _name: str) -> "_FakeSupabase":
        return self

    def table(self, _name: str) -> _Query:
        return _Query(self)


def test_is_season_complete_rpc_fast_path() -> None:
    sb = _FakeSupabase(rpc_data=0, total_count=10)
    assert _is_season_complete(sb, 4328, "2024-2025") is True


def test_is_season_complete_fallback_query_path() -> None:
    sb = _FakeSupabase(rpc_error=True, pending_count=3, total_count=10)
    assert _is_season_complete(sb, 4328, "2024-2025") is False


def test_is_season_complete_no_events_returns_false() -> None:
    sb = _FakeSupabase(rpc_data=0, total_count=0)
    assert _is_season_complete(sb, 4328, "2024-2025") is False

