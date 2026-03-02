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
- Daily update:
  - `python -m pipeline.runners.daily_update`
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
