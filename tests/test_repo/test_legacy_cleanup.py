"""tests/test_repo/test_legacy_cleanup.py"""
from __future__ import annotations

from pathlib import Path


def test_legacy_monolith_files_removed() -> None:
    root = Path(__file__).resolve().parents[2]
    assert not (root / "active.py").exists()
    assert not (root / "supabase_tables.py").exists()
    assert not (root / "requirements.txt").exists()


def test_old_runtime_artifact_directories_removed() -> None:
    root = Path(__file__).resolve().parents[2]
    assert not (root / "log_files").exists()
