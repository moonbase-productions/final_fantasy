# MUFL Spec Conformance Report

Date: 2026-03-01

## Scope

This report tracks conformance of the repository implementation to `PIPELINE_SPEC_FULL.md`, based on local static/runtime verification.

## Completed Locally

- Repository structure and file inventory from spec sections 5-11.
- Core pipeline modules:
  - config, DB factories, API client/endpoints/schemas
  - extract modules, transforms, upsert loader, SQL executor
  - runners, workflows, seed script, admin app
- SQL assets:
  - 17 derived query files in `src/pipeline/sql/queries/`
  - setup SQL files added in `sql/setup/` for permanent tables + `league_registry`
- Tests:
  - transform suite from spec plus added coverage for normalise
  - added coverage for events completeness behavior, endpoints, upsert behavior
  - added query inventory/order checks and workflow secret documentation checks
- Packaging/build:
  - build backend updated to `setuptools.build_meta`
  - wheel build validated locally (`pip wheel . --no-deps --no-build-isolation`)

## Verification Evidence

- Syntax checks:
  - `python3 -m compileall src scripts tests admin sql/setup` passes.
- Runtime smoke checks:
  - manual transform/integration-equivalent assertions pass.
  - manual checks for setup artifacts, endpoint URL builders, upsert retry behavior, SQL inventory, and runner SQL order pass.
- Runner preflight:
  - runners now fail fast with clear missing-env errors via `validate_runtime_settings()`.

## External Blockers (Not Runnable in This Environment)

- `pytest` unavailable and cannot be installed due restricted network access.
- Staging/live validations require real credentials and network:
  - full refresh run
  - daily update run
  - seed script against live TheSportsDB
  - admin UI write verification
  - GitHub Actions manual dispatch and secrets presence verification in GitHub UI

## Ready-to-Run Commands (When Env Is Available)

- `pip install -e .`
- `pip install -e ".[dev]"`
- `pytest tests/`
- `python scripts/seed_registry.py`
- `python -m pipeline.runners.full_refresh`
- `python -m pipeline.runners.daily_update`
- `streamlit run admin/app.py`
