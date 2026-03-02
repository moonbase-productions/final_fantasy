"""tests/test_ci/test_workflows.py"""
from __future__ import annotations

from pathlib import Path


REQUIRED_SECRETS = [
    "SPORTSDB_API_KEY",
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
]


def test_workflows_reference_required_secrets() -> None:
    root = Path(__file__).resolve().parents[2]
    workflows = [
        root / ".github" / "workflows" / "pipeline_weekly.yml",
        root / ".github" / "workflows" / "pipeline_daily.yml",
    ]
    for wf in workflows:
        content = wf.read_text(encoding="utf-8")
        for secret in REQUIRED_SECRETS:
            assert f"secrets.{secret}" in content, f"{secret} missing in {wf.name}"


def test_runbook_documents_required_secrets() -> None:
    root = Path(__file__).resolve().parents[2]
    content = (root / "RUNBOOK.md").read_text(encoding="utf-8")
    for secret in REQUIRED_SECRETS:
        assert f"`{secret}`" in content
