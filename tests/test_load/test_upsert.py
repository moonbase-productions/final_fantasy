"""tests/test_load/test_upsert.py"""
from __future__ import annotations

import pytest

from pipeline.load.upsert import batch_upsert


class _FakeQuery:
    def __init__(
        self,
        client: "_FakeClient",
        table_name: str,
        chunk: list[dict],
        on_conflict: str,
    ) -> None:
        self.client = client
        self.table_name = table_name
        self.chunk = chunk
        self.on_conflict = on_conflict

    def execute(self):
        self.client.calls.append(
            {
                "table": self.table_name,
                "size": len(self.chunk),
                "on_conflict": self.on_conflict,
                "rows": self.chunk,
            }
        )
        if self.client.failures_remaining > 0:
            self.client.failures_remaining -= 1
            raise RuntimeError("synthetic upsert failure")
        return {"ok": True}


class _FakeTable:
    def __init__(self, client: "_FakeClient", name: str) -> None:
        self.client = client
        self.name = name

    def upsert(self, chunk: list[dict], on_conflict: str) -> _FakeQuery:
        return _FakeQuery(self.client, self.name, chunk, on_conflict)


class _FakeClient:
    def __init__(self, failures_remaining: int = 0) -> None:
        self.failures_remaining = failures_remaining
        self.calls: list[dict] = []

    def table(self, name: str) -> _FakeTable:
        return _FakeTable(self, name)


def test_batch_upsert_deduplicates_and_chunks() -> None:
    client = _FakeClient()
    records = [
        {"id": 1, "name": "A"},
        {"id": 1, "name": "A-duplicate"},
        {"id": 2, "name": "B"},
        {"id": 3, "name": "C"},
    ]
    batch_upsert(client, "demo", records, "id", chunk_size=2)

    assert len(client.calls) == 2
    assert client.calls[0]["size"] == 2
    assert client.calls[1]["size"] == 1


def test_batch_upsert_retries_halved_chunks_on_failure() -> None:
    client = _FakeClient(failures_remaining=1)
    records = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
    ]
    batch_upsert(client, "demo", records, "id", chunk_size=4)

    # First full chunk fails once, then retried in two half chunks.
    assert [c["size"] for c in client.calls] == [4, 2, 2]


def test_batch_upsert_raises_on_second_failure() -> None:
    client = _FakeClient(failures_remaining=2)
    records = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
    ]
    with pytest.raises(RuntimeError):
        batch_upsert(client, "demo", records, "id", chunk_size=4)
