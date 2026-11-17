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
   - If `admin.league_registry` already existed before this point (i.e.
     you're updating an existing database rather than a fresh install),
     also run `sql/setup/03_team_source_override.sql` — it adds the
     `team_source_league_id` column via `ALTER TABLE ... ADD COLUMN IF
     NOT EXISTS`, safe to run any number of times.
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

## Team keying and the team-source override

Every team asset is keyed as `uid = "{league_id}-{team_id}"`, always using
the league you fetched it for — never the `idLeague` field TheSportsDB
returns on the team record. This is intentional: the same physical team
playing in two leagues (e.g. a club's domestic league and an international
competition) is meant to produce two separate asset rows, each with its
own stats/Elo history. TheSportsDB sometimes tags teams in aggregate or
multi-country leagues with their domestic league's id instead of the one
you actually queried — that's expected and handled automatically; it's
logged as a warning but never changes how the row is keyed.

If a league's own teams endpoint returns nothing usable at all (not just
mistagged, genuinely empty/broken), set `team_source_league_id` on that
league's row in `admin.league_registry` (via the Admin UI's League
Manager page, or direct SQL) to the league id teams should actually be
fetched from. The pipeline will fetch from that id but still store/key
the results under the original league's id, so events keep joining
correctly. Leave it blank for the normal case.

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
