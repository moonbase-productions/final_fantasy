# MUFL Pipeline Runbook

## First-Time Setup

1. Create and activate a Python 3.12 virtual environment.
2. Install package dependencies:
   - `pip install -e .`
   - `pip install -e ".[dev]"`
3. Create `.env` from `.env.example` and fill all required values.
4. In Supabase SQL Editor, run:
   - `sql/setup/01_permanent_tables.sql`
   - `sql/setup/02_league_registry.sql`
5. Seed whitelist/active leagues:
   - `python scripts/seed_registry.py`

## Local Command Matrix

- Full refresh:
  - `python -m pipeline.runners.full_refresh`
  - Also fetches `api.league_details` automatically, but only for leagues
    that don't have a row there yet (new leagues). Leagues that already
    have details are skipped, so this stays cheap on repeat runs.
- Daily update:
  - `python -m pipeline.runners.daily_update`
- Force-refresh ALL league details (manual only — never run automatically):
  - `python -m pipeline.runners.fetch_league_details`
  - Re-fetches `api.league_details` for every league in `api.leagues`,
    including ones that already have a row (e.g. to pick up a rename or
    fix stale data). One API call per league, so this can take a long
    time given the rate limit. Run on demand only; not part of full
    refresh, daily update, or any scheduled workflow.
- Seed registry:
  - `python scripts/seed_registry.py`
- Admin app:
  - `pip install -r admin/requirements.txt`
  - `streamlit run admin/app.py`
- Unit/integration tests:
  - `pytest tests/`

## GitHub Actions

- Weekly full refresh workflow:
  - `.github/workflows/pipeline_weekly.yml`
- 4-hour daily update workflow:
  - `.github/workflows/pipeline_daily.yml`

Required secrets:
- `SPORTSDB_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
