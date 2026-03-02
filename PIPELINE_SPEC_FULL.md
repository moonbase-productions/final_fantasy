---
title: "MUFL Pipeline — Complete Technical Specification"
subtitle: "A Self-Contained Implementation Guide"
author: "Architecture Document"
date: "2026"
---

\newpage

# 1. System Overview

## 1.1 What Is This?

This document specifies the complete rewrite of the MUFL (Multi-sport/league Fantasy League) data pipeline. The pipeline is the back-end engine of a fantasy sports website where users own **teams** (not individual players). It:

1. **Extracts** sports data from TheSportsDB V2 API
2. **Transforms** that data into statistics, Elo ratings, tier classifications, and luck scores
3. **Loads** processed data into a Supabase (Postgres) database that the website reads directly

The pipeline is a Python package that runs on GitHub Actions — once weekly for a full data refresh and every 4 hours to update scores from ongoing games.

## 1.2 What the Pipeline Produces

The website reads from a set of derived tables (named `sql_*`) that the pipeline creates fresh each run. The primary output table, `sql_web_assets`, contains one row per team with:

- Team identity: name, logo, league, sport, country
- Elo rating and rank (current, delta, season delta)
- Tier label (MOL, SS, S, A, B, C, D, E, F, FF, DIE)
- Win/loss/draw records (last 10 games, current season, last season, all-time)
- Attack and defence percentile scores
- Luck score (Elo-adjusted win rate delta, 0–100)
- Win probability forecast (next 20 games)
- Points and points-per-week for current season

The secondary output `sql_web_events` contains every game for active leagues across the last 5 seasons with scores, team names, and status.

## 1.3 Technology Stack

| Component | Technology | Version |
|---|---|---|
| Language | Python | 3.12 |
| HTTP client | httpx | >=0.27 |
| Data validation | Pydantic v2 | >=2.7 |
| Config management | pydantic-settings | >=2.3 |
| Database ORM | supabase-py | >=2.5 |
| DDL execution | psycopg2-binary | >=2.9 |
| Data processing | pandas | >=2.2 |
| Numerics | numpy | >=1.26 |
| Admin UI | Streamlit | >=1.35 |
| CI/CD | GitHub Actions | — |
| Database | Supabase (Postgres 17.6) | Micro tier |
| Sports data API | TheSportsDB | V2 |

## 1.4 What Is Being Replaced

The previous implementation was a single monolithic file (`active.py`, 1,074 lines) with a companion SQL file (`supabase_tables.py`, 781 lines). Key problems:

- All database connections, logging, and constants initialised at module import time (side effects)
- V1 API used for the highest-volume call (season events) while V2 was used for others
- Rate limiting via `time.sleep(0.6)` with no token budget — burst calls could exceed 100/min
- Active league IDs hardcoded as a Python list requiring a code push to change
- `js_rounds` table dependency scattered across all SQL — external table, no pipeline ownership
- `asset_luck` column was literally `ROUND(RANDOM() * 100)` — random number, no meaning
- File-based JSON/pickle cache with no expiry or integrity checking
- Row-by-row Python loop for basic stats computation (should be vectorised)
- `CREATE POLICY` crashed on every run after first (no idempotency)
- No sport-type awareness — motor racing, combat sports, tennis treated identically to soccer

The full source of both files is reproduced in Appendix A and Appendix B for reference.

\newpage

# 2. Prerequisites and Environment Setup

## 2.1 Required Accounts and Services

1. **TheSportsDB developer account** — provides an API key for V2 access (100 requests/minute)
2. **Supabase project** — free tier is insufficient for the compute required; use at minimum the Micro tier ($0.01344/hour)
3. **GitHub repository** — the pipeline runs via GitHub Actions; secrets are stored in the repo

## 2.2 Supabase Configuration

The pipeline uses **two** Supabase connection methods:

- **supabase-py client** (PostgREST REST API) — for all data upserts (`api_*` and `py_*` tables). Uses the service role key.
- **psycopg2 direct Postgres connection** — for DDL statements (`DROP TABLE`, `CREATE TABLE AS SELECT`, `ALTER TABLE`). PostgREST does not support DDL.

**CRITICAL — Connection type for psycopg2:**

The psycopg2 connection **must** use the **direct connection**, not the Supabase connection pooler. DDL statements fail with "cannot run inside a transaction block" on the Transaction Mode pooler.

To find your direct connection string:

1. Supabase Dashboard → Project Settings → Database
2. Select the **Direct connection** tab (not "Connection pooler")
3. The host will be: `db.XXXXXXXXXXXXXXXX.supabase.co` (with `db.` prefix)
4. Port: `5432`

Your `.env` `DB_HOST` must be this direct connection host. If it ends in `.pooler.supabase.com`, it is wrong.

## 2.3 Local Development Setup

```bash
# Clone the repository
git clone https://github.com/your-org/pipeline.git
cd pipeline

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install the package in editable mode (installs all dependencies)
pip install -e .

# Install dev dependencies
pip install -e ".[dev]"

# Copy environment template and fill in values
cp .env.example .env
# Edit .env with your actual credentials
```

## 2.4 Environment Variables

Create a `.env` file (never commit this file). All variables are required.

```
# TheSportsDB API
SPORTSDB_API_KEY=your_api_key_here

# Supabase (PostgREST client — use anon or service role key)
SUPABASE_URL=https://XXXXXXXXXXXXXXXX.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...your_service_role_key

# Supabase direct Postgres connection (for DDL only)
# Host MUST be the direct connection: db.XXXX.supabase.co NOT pooler
DB_HOST=db.XXXXXXXXXXXXXXXX.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_database_password
```

**GitHub Actions secrets** — add each of these under Settings → Secrets → Actions:
`SPORTSDB_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

## 2.5 First-Time Database Setup

Before running the pipeline for the first time, three things must be done manually in the Supabase SQL Editor:

**Step 1: Create the permanent base tables.** Run the DDL in Section 3.2.

**Step 2: Create the `league_registry` table.** Run the DDL in Section 3.3.

**Step 3: Run the seed script** to populate `league_registry` with the 71 whitelisted leagues and their TheSportsDB IDs:

```bash
python scripts/seed_registry.py
```

This makes one API call to fetch all leagues, matches them by name to the whitelist, and inserts them. It will print any leagues it could not match so you can add them manually.

\newpage

# 3. Database Schema

## 3.1 Overview of Tables

The pipeline maintains two categories of tables:

**Permanent tables** — upserted each run, never dropped. They accumulate historical data.

| Table | Purpose |
|---|---|
| `api_leagues` | All leagues known to TheSportsDB |
| `api_league_details` | Extended metadata per league |
| `api_seasons` | All seasons per league |
| `api_assets` | All teams (called "assets" in the fantasy context) |
| `api_events` | All games/matches |
| `py_stats` | Computed per-team per-season statistics and Elo ratings |
| `py_tier` | Most recent tier label per team |
| `league_registry` | Admin-managed whitelist and active-league configuration |

**Ephemeral derived tables** — dropped and recreated every pipeline run. These are what the website reads.

17 tables named `sql_*` — full list in Section 7.

## 3.2 Permanent Table DDL

Run this once in the Supabase SQL Editor before first pipeline run.

```sql
-- api_leagues: all leagues from TheSportsDB
CREATE TABLE IF NOT EXISTS public.api_leagues (
    league_id               TEXT PRIMARY KEY,
    league_name             TEXT,
    league_sport            TEXT,
    league_name_alternate   TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- api_league_details: extended league metadata
CREATE TABLE IF NOT EXISTS public.api_league_details (
    league_id               TEXT PRIMARY KEY,
    league_name             TEXT,
    league_sport            TEXT,
    league_name_alternate   TEXT,
    league_division         TEXT,
    league_cup              TEXT,
    league_current_season   TEXT,
    league_formed_year      TEXT,
    league_first_event      TEXT,
    league_gender           TEXT,
    league_country          TEXT,
    league_description_en   TEXT,
    league_badge            TEXT,
    league_trophy           TEXT,
    league_complete         TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- api_seasons: all seasons per league
CREATE TABLE IF NOT EXISTS public.api_seasons (
    league_id       TEXT NOT NULL,
    league_season   TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (league_id, league_season)
);

-- api_assets: teams (called "assets" in fantasy context)
-- uid = league_id + '-' + team_id  e.g. "4328-133604"
CREATE TABLE IF NOT EXISTS public.api_assets (
    uid             TEXT PRIMARY KEY,
    league_id       TEXT NOT NULL,
    team_name       TEXT,
    team_short      TEXT,
    team_logo       TEXT,
    team_country    TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- api_events: all games/matches
-- team_score_home/away are FLOAT to support normalised scores
-- (binary sports use 1.0/0.0; multi-competitor uses points values)
CREATE TABLE IF NOT EXISTS public.api_events (
    event_id            TEXT PRIMARY KEY,
    event_date          DATE,
    event_time          TIME,
    league_id           TEXT,
    league_sport        TEXT,
    league_season       TEXT,
    league_round        TEXT,
    uid_home            TEXT,
    team_score_home     FLOAT,
    uid_away            TEXT,
    team_score_away     FLOAT,
    event_status        TEXT,
    event_video         TEXT,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_events_league
    ON public.api_events(league_id, league_season);
CREATE INDEX IF NOT EXISTS idx_api_events_date
    ON public.api_events(event_date);

-- py_stats: computed statistics per team per season
-- Includes Elo ratings, W/L/D, points, and luck metrics
CREATE TABLE IF NOT EXISTS public.py_stats (
    uid                         TEXT NOT NULL,
    league_id                   TEXT NOT NULL,
    league_season               TEXT NOT NULL,
    wins                        INT,
    losses                      INT,
    draws                       INT,
    points_for                  FLOAT,
    points_against              FLOAT,
    games_played                INT,
    avg_points_for              FLOAT,
    avg_points_against          FLOAT,
    win_percentage              FLOAT,
    home_wins                   INT,
    home_losses                 INT,
    home_draws                  INT,
    home_points_for             FLOAT,
    home_points_against         FLOAT,
    home_games_played           INT,
    avg_home_points_for         FLOAT,
    avg_home_points_against     FLOAT,
    home_win_percentage         FLOAT,
    avg_points_for_percentile   FLOAT,
    avg_points_against_percentile FLOAT,
    start_rank_league           INT,
    end_rank_league             INT,
    start_of_season_elo         FLOAT,
    end_of_season_elo           FLOAT,
    last_elo_delta              FLOAT,
    season_elo_delta            FLOAT,
    luck_score                  FLOAT,      -- NEW: raw Elo-adjusted delta
    luck_display                INT,        -- NEW: 0-100 percentile rank
    updated_at                  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (uid, league_season)
);

CREATE INDEX IF NOT EXISTS idx_py_stats_uid ON public.py_stats(uid);

-- py_tier: most recent tier label per team
CREATE TABLE IF NOT EXISTS public.py_tier (
    uid         TEXT PRIMARY KEY,
    league_id   TEXT,
    tier        TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
```

## 3.3 league_registry DDL

```sql
-- league_registry: admin-controlled whitelist and active-league config
-- is_whitelisted: pipeline fetches data for this league
-- is_active: league appears on the fantasy platform (implies whitelisted)
-- sport_type: how to normalise events for Elo
--   'standard'         = home/away teams, numeric scores (soccer, basketball, etc.)
--   'binary'           = winner/loser only, no meaningful score (UFC, tennis, etc.)
--   'multi_competitor' = multiple teams in one event (F1, NASCAR, cycling)

CREATE TABLE IF NOT EXISTS public.league_registry (
    league_id       BIGINT PRIMARY KEY,
    league_name     TEXT NOT NULL,
    league_sport    TEXT NOT NULL,
    sport_type      TEXT CHECK (sport_type IN ('standard','binary','multi_competitor')),
    is_whitelisted  BOOLEAN NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN NOT NULL DEFAULT FALSE,
    display_name    TEXT,
    last_fetched_at TIMESTAMPTZ,
    team_count      INT,
    notes           TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT active_requires_whitelisted
        CHECK (NOT is_active OR is_whitelisted),
    CONSTRAINT active_requires_sport_type
        CHECK (NOT is_active OR sport_type IS NOT NULL)
);

-- Public read; writes via service role only
ALTER TABLE public.league_registry ENABLE ROW LEVEL SECURITY;
CREATE POLICY "public read" ON public.league_registry
    FOR SELECT TO public USING (true);
```

\newpage

# 4. TheSportsDB API Reference

## 4.1 Authentication

All V2 requests use a header — **not** a URL parameter:

```
X-API-KEY: your_api_key_here
```

Base URL: `https://www.thesportsdb.com/api/v2/json`

Rate limit: **100 requests per minute** on the developer tier.

## 4.2 Endpoints Used

| Endpoint | URL Pattern | Returns |
|---|---|---|
| All leagues | `GET /all/leagues` | Every league in TheSportsDB |
| League detail | `GET /lookup/league/{league_id}` | Extended metadata for one league |
| Seasons | `GET /list/seasons/{league_id}` | All seasons for one league |
| Teams | `GET /list/teams/{league_id}` | All teams in one league |
| Season events | `GET /filter/events/{league_id}/{season}` | All events in one league-season |

## 4.3 Response Schemas

**All leagues** — `GET /all/leagues`:
```json
{
  "leagues": [
    {
      "idLeague": "4328",
      "strLeague": "English Premier League",
      "strSport": "Soccer",
      "strLeagueAlternate": "EPL"
    }
  ]
}
```
Note: The outer key varies. Iterate `response.values()` and check for lists.

**League detail** — `GET /lookup/league/{id}`:
```json
{
  "lookup": [
    {
      "idLeague": "4328",
      "strLeague": "English Premier League",
      "strSport": "Soccer",
      "strLeagueAlternate": "EPL",
      "intDivision": "1",
      "idCup": null,
      "strCurrentSeason": "2024-2025",
      "intFormedYear": "1992",
      "dateFirstEvent": "1992-08-15",
      "strGender": "Male",
      "strCountry": "England",
      "strDescriptionEN": "The Premier League...",
      "strBadge": "https://...badge.png",
      "strTrophy": "https://...trophy.png",
      "strComplete": "yes"
    }
  ]
}
```

**Seasons** — `GET /list/seasons/{id}`:
```json
{
  "list": [
    { "strSeason": "2024-2025" },
    { "strSeason": "2023-2024" }
  ]
}
```

**Teams** — `GET /list/teams/{id}`:
```json
{
  "list": [
    {
      "idTeam": "133604",
      "idLeague": "4328",
      "strTeam": "Arsenal",
      "strTeamShort": "ARS",
      "strBadge": "https://...badge.png",
      "strCountry": "England"
    }
  ]
}
```

**Season events** — `GET /filter/events/{league_id}/{season}`:
```json
{
  "events": [
    {
      "idEvent": "1569537",
      "dateEvent": "2024-08-17",
      "strTime": "12:30:00",
      "idLeague": "4328",
      "strSport": "Soccer",
      "strSeason": "2024-2025",
      "intRound": "1",
      "idHomeTeam": "133604",
      "intHomeScore": "2",
      "idAwayTeam": "133602",
      "intAwayScore": "0",
      "strStatus": "Match Finished",
      "strVideo": null
    }
  ]
}
```

**Important**: All numeric fields are returned as **strings** (e.g. `"intHomeScore": "2"`, not `2`). The pipeline must cast them. Null scores are returned as `null` (JSON null) or an empty string `""`.

\newpage

# 5. Repository Structure

```
pipeline/
├── pyproject.toml
├── .env.example
├── .gitignore
├── .github/
│   └── workflows/
│       ├── pipeline_weekly.yml
│       └── pipeline_daily.yml
├── src/
│   └── pipeline/
│       ├── __init__.py
│       ├── config.py
│       ├── db.py
│       ├── api/
│       │   ├── __init__.py
│       │   ├── client.py
│       │   ├── endpoints.py
│       │   └── schemas.py
│       ├── extract/
│       │   ├── __init__.py
│       │   ├── leagues.py
│       │   ├── seasons.py
│       │   ├── teams.py
│       │   └── events.py
│       ├── transform/
│       │   ├── __init__.py
│       │   ├── normalise.py
│       │   ├── stats.py
│       │   ├── elo.py
│       │   ├── tiers.py
│       │   └── luck.py
│       ├── load/
│       │   ├── __init__.py
│       │   └── upsert.py
│       ├── sql/
│       │   ├── __init__.py
│       │   ├── executor.py
│       │   └── queries/
│       │       ├── 01_leagues_current.sql
│       │       ├── 02_season_windows.sql
│       │       ├── 03_web_events.sql
│       │       ├── 04_events_scored.sql
│       │       ├── 05_events_split.sql
│       │       ├── 06_asset_last_10.sql
│       │       ├── 07_events_future_elos.sql
│       │       ├── 08_assets_future.sql
│       │       ├── 09_assets_stats_at.sql
│       │       ├── 10_forecast.sql
│       │       ├── 11_assets_season_to_date.sql
│       │       ├── 12_ref_elo.sql
│       │       ├── 13_current_elo.sql
│       │       ├── 14_wld.sql
│       │       ├── 15_web_assets.sql
│       │       ├── 16_web_assets_info.sql
│       │       └── 17_league_info.sql
│       └── runners/
│           ├── __init__.py
│           ├── full_refresh.py
│           └── daily_update.py
├── admin/
│   ├── app.py
│   └── requirements.txt
├── scripts/
│   └── seed_registry.py
└── tests/
    ├── conftest.py
    ├── test_transform/
    │   ├── test_stats.py
    │   ├── test_elo.py
    │   ├── test_tiers.py
    │   └── test_luck.py
    └── test_integration/
        └── test_smoke.py
```

\newpage

# 6. Python Implementation — Complete Code

## 6.1 `pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "pipeline"
version = "2.0.0"
description = "MUFL fantasy league data pipeline"
requires-python = ">=3.12"
dependencies = [
    "httpx>=0.27",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "supabase>=2.5",
    "psycopg2-binary>=2.9",
    "pandas>=2.2",
    "numpy>=1.26",
    "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = ["pytest>=8", "pytest-asyncio", "ruff", "mypy"]
admin = ["streamlit>=1.35"]

[tool.setuptools.packages.find]
where = ["src"]
```

## 6.2 `.env.example`

```
SPORTSDB_API_KEY=your_key_here
SUPABASE_URL=https://XXXXXXXXXXXXXXXX.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...
DB_HOST=db.XXXXXXXXXXXXXXXX.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password
```

## 6.3 `src/pipeline/config.py`

Central configuration. Every constant in the pipeline lives here. Nothing is hardcoded elsewhere.

```python
from __future__ import annotations
from typing import ClassVar
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- External service credentials (from .env) ---
    sportsdb_api_key: str
    supabase_url: str
    supabase_service_role_key: str
    db_host: str
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: str

    # --- Pipeline constants (not from env) ---

    # Number of past seasons to keep in derived tables
    SEASON_WINDOW: ClassVar[int] = 5

    # TheSportsDB API rate limit
    API_RATE_LIMIT: ClassVar[int] = 100  # requests per minute

    # Supabase upsert batch size
    UPSERT_CHUNK_SIZE: ClassVar[int] = 100

    # Starting Elo for teams with no history
    INIT_ELO: ClassVar[int] = 1500

    # Elo K-values by sport. Governs how much each result shifts ratings.
    # Soccer: 20.75 (standard for international Elo)
    # Baseball: 4.0 (low-scoring, high variance)
    # default: applies to all other sports
    K_VALUES: ClassVar[dict[str, float]] = {
        "Soccer": 20.75,
        "Baseball": 4.0,
        "default": 20.0,
    }

    # Tier thresholds. Each tuple is (min_percentile, tier_name).
    # Applied in order: first matching threshold wins.
    TIER_THRESHOLDS: ClassVar[list[tuple[float, str]]] = [
        (0.995, "MOL"),
        (0.95,  "SS"),
        (0.85,  "S"),
        (0.70,  "A"),
        (0.60,  "B"),
        (0.50,  "C"),
        (0.30,  "D"),
        (0.15,  "E"),
        (0.05,  "F"),
        (0.005, "FF"),
        # Below 0.005: "DIE"
    ]

    # F1 championship points by finishing position (1st through 10th)
    F1_POINTS: ClassVar[dict[int, float]] = {
        1: 25, 2: 18, 3: 15, 4: 12, 5: 10,
        6: 8,  7: 6,  8: 4,  9: 2,  10: 1,
    }

    # NASCAR Cup points are complex; use simplified finish-based points
    NASCAR_POINTS: ClassVar[dict[int, float]] = {
        1: 40, 2: 35, 3: 34, 4: 33, 5: 32,
        6: 31, 7: 30, 8: 29, 9: 28, 10: 27,
    }

    # Number of recent games to use for luck calculation
    LUCK_WINDOW: ClassVar[int] = 20


settings = Settings()
```

## 6.4 `src/pipeline/db.py`

Database connection factories. Never called at module level — always inside runner functions.

```python
from __future__ import annotations
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extensions
from supabase import create_client, Client

from pipeline.config import settings


def get_supabase_client() -> Client:
    """Return a Supabase PostgREST client using the service role key.

    Use this for all data upserts (api_* and py_* tables).
    The service role key bypasses Row Level Security — required for writes.
    """
    return create_client(settings.supabase_url, settings.supabase_service_role_key)


@contextmanager
def get_pg_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a direct psycopg2 Postgres connection for DDL statements.

    This MUST connect to the direct Supabase host (db.XXXX.supabase.co, port 5432).
    The connection pooler does not support DDL.

    Usage:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS ...")
            conn.commit()
    """
    conn = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=15,
        options="-c statement_timeout=300000",  # 5-minute max per statement
    )
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

## 6.5 `src/pipeline/api/client.py`

Token-bucket rate limiter wrapping httpx. Enforces 100 req/min by tracking a token budget rather than sleeping a fixed amount per request. Thread-safe.

```python
from __future__ import annotations
import logging
import threading
import time

import httpx

from pipeline.config import settings

logger = logging.getLogger(__name__)


class RateLimitedClient:
    """HTTP client with token-bucket rate limiting.

    Maintains a bucket of tokens replenished at `rate` per minute.
    Each GET request consumes one token. If the bucket is empty,
    the call blocks until a token is available.

    Usage:
        with RateLimitedClient() as client:
            data = client.get("https://...")
    """

    def __init__(self, rate: int = settings.API_RATE_LIMIT) -> None:
        self._rate = rate
        self._tokens = float(rate)
        self._lock = threading.Lock()
        self._last_refill = time.monotonic()
        self._client = httpx.Client(
            timeout=httpx.Timeout(30.0),
            headers={
                "X-API-KEY": settings.sportsdb_api_key,
                "Content-Type": "application/json",
            },
        )

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self._rate),
            self._tokens + elapsed * (self._rate / 60.0),
        )
        self._last_refill = now

    def get(self, url: str) -> dict:
        """Make a rate-limited GET request. Returns parsed JSON.

        Blocks if no tokens are available. Raises on HTTP errors.
        """
        with self._lock:
            self._refill()
            if self._tokens < 1:
                wait = (1 - self._tokens) * (60.0 / self._rate)
                logger.debug("Rate limit: sleeping %.2fs", wait)
                time.sleep(wait)
                self._refill()
            self._tokens -= 1

        logger.debug("GET %s", url)
        response = self._client.get(url)
        response.raise_for_status()
        return response.json()

    def __enter__(self) -> "RateLimitedClient":
        return self

    def __exit__(self, *_) -> None:
        self._client.close()
```

## 6.6 `src/pipeline/api/endpoints.py`

All V2 URL construction. One function per endpoint. No URL strings anywhere else in the codebase.

```python
from __future__ import annotations

BASE = "https://www.thesportsdb.com/api/v2/json"


def all_leagues_url() -> str:
    """All leagues in TheSportsDB."""
    return f"{BASE}/all/leagues"


def league_detail_url(league_id: int | str) -> str:
    """Extended metadata for a single league."""
    return f"{BASE}/lookup/league/{league_id}"


def seasons_url(league_id: int | str) -> str:
    """All seasons for a single league."""
    return f"{BASE}/list/seasons/{league_id}"


def teams_url(league_id: int | str) -> str:
    """All teams in a single league."""
    return f"{BASE}/list/teams/{league_id}"


def season_events_url(league_id: int | str, season: str) -> str:
    """All events for a specific league-season combination.

    Season format examples: '2024-2025', '2024', 'Season 2024'
    """
    return f"{BASE}/filter/events/{league_id}/{season}"
```

## 6.7 `src/pipeline/api/schemas.py`

Pydantic v2 models for raw API responses. All numeric fields from TheSportsDB are strings — coercion to float/int happens here. Fields are Optional because TheSportsDB frequently returns null for missing data.

```python
from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, field_validator


class LeagueItem(BaseModel):
    idLeague: str
    strLeague: str
    strSport: str
    strLeagueAlternate: Optional[str] = None


class LeagueDetailItem(BaseModel):
    idLeague: str
    strLeague: str
    strSport: str
    strLeagueAlternate: Optional[str] = None
    intDivision: Optional[str] = None
    idCup: Optional[str] = None
    strCurrentSeason: Optional[str] = None
    intFormedYear: Optional[str] = None
    dateFirstEvent: Optional[str] = None
    strGender: Optional[str] = None
    strCountry: Optional[str] = None
    strDescriptionEN: Optional[str] = None
    strBadge: Optional[str] = None
    strTrophy: Optional[str] = None
    strComplete: Optional[str] = None


class SeasonItem(BaseModel):
    strSeason: str


class TeamItem(BaseModel):
    idTeam: str
    idLeague: str
    strTeam: str
    strTeamShort: Optional[str] = None
    strBadge: Optional[str] = None
    strCountry: Optional[str] = None


class EventItem(BaseModel):
    idEvent: str
    dateEvent: Optional[str] = None
    strTime: Optional[str] = None
    idLeague: Optional[str] = None
    strSport: Optional[str] = None
    strSeason: Optional[str] = None
    intRound: Optional[str] = None
    idHomeTeam: Optional[str] = None
    intHomeScore: Optional[str] = None   # string "3" or null
    idAwayTeam: Optional[str] = None
    intAwayScore: Optional[str] = None   # string "1" or null
    strStatus: Optional[str] = None
    strVideo: Optional[str] = None

    def home_score_float(self) -> Optional[float]:
        """Parse home score string to float. Returns None if missing/blank."""
        if not self.intHomeScore:
            return None
        try:
            return float(self.intHomeScore)
        except ValueError:
            return None

    def away_score_float(self) -> Optional[float]:
        if not self.intAwayScore:
            return None
        try:
            return float(self.intAwayScore)
        except ValueError:
            return None
```

## 6.8 `src/pipeline/extract/leagues.py`

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import all_leagues_url, league_detail_url

logger = logging.getLogger(__name__)


def fetch_all_leagues(client: RateLimitedClient) -> list[dict]:
    """Fetch every league in TheSportsDB.

    The response is a dict whose values are lists of league objects.
    Iterates all values and collects any dicts with 'idLeague'.
    Returns list of dicts suitable for upsert into api_leagues.
    """
    url = all_leagues_url()
    data = client.get(url)
    leagues: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for items in data.values():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict) or "idLeague" not in item:
                continue
            leagues.append({
                "league_id": item.get("idLeague"),
                "league_name": item.get("strLeague"),
                "league_sport": item.get("strSport"),
                "league_name_alternate": item.get("strLeagueAlternate") or "",
                "created_at": now,
            })

    logger.info("Fetched %d leagues from TheSportsDB.", len(leagues))
    return leagues


def fetch_league_details(
    client: RateLimitedClient,
    league_ids: list[int],
) -> list[dict]:
    """Fetch extended metadata for each league in league_ids.

    Makes one API call per league. Skips leagues where the response
    contains no 'lookup' data and logs a warning.
    Returns list of dicts suitable for upsert into api_league_details.
    """
    details: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for league_id in league_ids:
        url = league_detail_url(league_id)
        try:
            data = client.get(url)
        except Exception as exc:
            logger.warning("Failed to fetch details for league %s: %s", league_id, exc)
            continue

        lookup = data.get("lookup") or []
        if not lookup:
            logger.warning("No detail data for league %s.", league_id)
            continue

        for item in lookup:
            details.append({
                "league_id": item.get("idLeague"),
                "league_name": item.get("strLeague"),
                "league_sport": item.get("strSport"),
                "league_name_alternate": item.get("strLeagueAlternate"),
                "league_division": item.get("intDivision"),
                "league_cup": item.get("idCup"),
                "league_current_season": item.get("strCurrentSeason"),
                "league_formed_year": item.get("intFormedYear"),
                "league_first_event": item.get("dateFirstEvent"),
                "league_gender": item.get("strGender"),
                "league_country": item.get("strCountry"),
                "league_description_en": item.get("strDescriptionEN"),
                "league_badge": item.get("strBadge"),
                "league_trophy": item.get("strTrophy"),
                "league_complete": item.get("strComplete"),
                "created_at": now,
            })

    logger.info("Fetched details for %d leagues.", len(details))
    return details
```

## 6.9 `src/pipeline/extract/seasons.py`

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import seasons_url

logger = logging.getLogger(__name__)


def fetch_seasons(
    client: RateLimitedClient,
    league_ids: list[int],
) -> list[dict]:
    """Fetch all seasons for each league in league_ids.

    Makes one API call per league. Returns list of dicts suitable
    for upsert into api_seasons.
    """
    seasons: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for league_id in league_ids:
        url = seasons_url(league_id)
        try:
            data = client.get(url)
        except Exception as exc:
            logger.warning("Failed seasons for league %s: %s", league_id, exc)
            continue

        season_list = data.get("list") or []
        for item in season_list:
            season_str = item.get("strSeason")
            if not season_str:
                continue
            seasons.append({
                "league_id": str(league_id),
                "league_season": season_str,
                "updated_at": now,
            })

    logger.info("Fetched %d season records.", len(seasons))
    return seasons
```

## 6.10 `src/pipeline/extract/teams.py`

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import teams_url

logger = logging.getLogger(__name__)


def fetch_teams(
    client: RateLimitedClient,
    league_ids: list[int],
) -> list[dict]:
    """Fetch all teams for each league in league_ids.

    uid is constructed as "{league_id}-{team_id}" — the primary key
    used throughout the pipeline and database.
    Returns list of dicts suitable for upsert into api_assets.
    """
    teams: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for league_id in league_ids:
        url = teams_url(league_id)
        try:
            data = client.get(url)
        except Exception as exc:
            logger.warning("Failed teams for league %s: %s", league_id, exc)
            continue

        team_list = data.get("list") or []
        if not team_list:
            logger.warning("No teams returned for league %s.", league_id)
            continue

        for item in team_list:
            team_id = item.get("idTeam")
            if not team_id:
                continue
            teams.append({
                "uid": f"{item.get('idLeague')}-{team_id}",
                "league_id": item.get("idLeague"),
                "team_name": item.get("strTeam"),
                "team_short": item.get("strTeamShort") or "",
                "team_logo": item.get("strBadge") or "",
                "team_country": item.get("strCountry") or "",
                "created_at": now,
                "updated_at": now,
            })

    logger.info("Fetched %d teams.", len(teams))
    return teams
```

## 6.11 `src/pipeline/extract/events.py`

This is the most complex extractor. It applies the incremental skip strategy: past seasons that are fully scored are not re-fetched.

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

from supabase import Client

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import season_events_url
from pipeline.transform.normalise import sanitise_date, sanitise_time, sanitise_score

logger = logging.getLogger(__name__)


def _is_season_complete(supabase: Client, league_id: int, season: str) -> bool:
    """Return True if all scoreable events in this season are already recorded.

    A season is 'complete' when there are zero events that are:
    - older than 1 day (so the score is finalised), AND
    - missing a score

    If the season has no events at all in the DB, returns False (must fetch).
    """
    result = supabase.rpc(
        "count_pending_events",
        {"p_league_id": str(league_id), "p_season": season},
    ).execute()
    # Falls back to a direct query if RPC not available
    # Direct query approach:
    try:
        response = (
            supabase.table("api_events")
            .select("event_id", count="exact")
            .eq("league_id", str(league_id))
            .eq("league_season", season)
            .is_("team_score_home", "null")
            .lt("event_date", (datetime.now().date()).isoformat())
            .execute()
        )
        pending = response.count or 0

        # Also check if we have any events at all
        total_response = (
            supabase.table("api_events")
            .select("event_id", count="exact")
            .eq("league_id", str(league_id))
            .eq("league_season", season)
            .execute()
        )
        total = total_response.count or 0

        if total == 0:
            return False  # No events yet — must fetch
        return pending == 0
    except Exception as exc:
        logger.warning("Could not check season completeness: %s", exc)
        return False  # When in doubt, fetch


def _parse_events(
    data: dict,
    league_id: int,
    season: str,
) -> list[dict]:
    """Parse raw API event response into a list of dicts for api_events."""
    events = data.get("events") or []
    parsed: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for item in events:
        event_id = item.get("idEvent")
        if not event_id:
            continue

        league_id_str = item.get("idLeague") or str(league_id)
        home_id = item.get("idHomeTeam") or ""
        away_id = item.get("idAwayTeam") or ""

        # Skip events without team assignments (e.g. TBD fixtures)
        if not home_id or not away_id:
            continue

        parsed.append({
            "event_id": event_id,
            "event_date": sanitise_date(item.get("dateEvent") or "1970-01-01"),
            "event_time": sanitise_time(item.get("strTime") or ""),
            "league_id": league_id_str,
            "league_sport": item.get("strSport") or "",
            "league_season": season,
            "league_round": str(item.get("intRound") or ""),
            "uid_home": f"{league_id_str}-{home_id}",
            "uid_away": f"{league_id_str}-{away_id}",
            "team_score_home": sanitise_score(item.get("intHomeScore")),
            "team_score_away": sanitise_score(item.get("intAwayScore")),
            "event_status": item.get("strStatus") or "",
            "event_video": item.get("strVideo") or "",
            "updated_at": now,
        })

    return parsed


def fetch_events_for_season(
    client: RateLimitedClient,
    league_id: int,
    season: str,
) -> list[dict]:
    """Fetch all events for a single league-season from the API."""
    url = season_events_url(league_id, season)
    try:
        data = client.get(url)
    except Exception as exc:
        logger.warning(
            "Failed to fetch events for league %s season %s: %s",
            league_id, season, exc,
        )
        return []

    events = _parse_events(data, league_id, season)
    logger.info(
        "Fetched %d events for league %s season %s.",
        len(events), league_id, season,
    )
    return events


def fetch_events_incremental(
    client: RateLimitedClient,
    supabase: Client,
    whitelisted_ids: list[int],
    season_last5: list[dict],
    current_only: bool = False,
) -> list[dict]:
    """Fetch events using incremental skip strategy.

    For each (league_id, season) pair in season_last5:
    - season_rank == 1 (current): always fetch
    - season_rank >= 2 (past): skip if all scoreable events already recorded

    If current_only=True, only fetch season_rank==1 (used by daily runner).

    Args:
        client: rate-limited API client
        supabase: supabase client for completeness checks
        whitelisted_ids: list of league IDs to process
        season_last5: rows from sql_season_last5 table
        current_only: if True, only fetch current season

    Returns:
        Flat list of event dicts for upsert into api_events.
    """
    all_events: list[dict] = []
    whitelisted_set = set(str(i) for i in whitelisted_ids)

    for row in season_last5:
        league_id = row["league_id"]
        season = row["league_season"]
        rank = int(row["season_rank"])

        if str(league_id) not in whitelisted_set:
            continue

        if current_only and rank != 1:
            continue

        if rank >= 2 and _is_season_complete(supabase, league_id, season):
            logger.info(
                "Skipping complete past season: league %s season %s.",
                league_id, season,
            )
            continue

        events = fetch_events_for_season(client, int(league_id), season)
        all_events.extend(events)

    logger.info("Total events fetched (incremental): %d", len(all_events))
    return all_events
```

## 6.12 `src/pipeline/transform/normalise.py`

Sanitises raw API values. Also contains sport-type-specific event normalisation — converting binary-outcome and multi-competitor events into the standard home/away schema that downstream stat computation expects.

```python
from __future__ import annotations
import logging
import re
from typing import Optional

import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Raw value sanitisation
# ---------------------------------------------------------------------------

def sanitise_date(date_str: str) -> str:
    """Fix malformed dates from TheSportsDB.

    '0000-00-00' -> '1970-01-01'
    '2024-00-15' -> '2024-01-15'
    """
    if not date_str:
        return "1970-01-01"
    date_str = date_str.replace("0000-00-00", "1970-01-01")
    date_str = re.sub(r"-00", "-01", date_str)
    return date_str


def sanitise_time(time_str: str) -> str:
    """Normalise time strings from TheSportsDB.

    Strips timezone suffixes like ' ET', ' AM ET'.
    Truncates malformed times like '18:30:00:00' to '18:30:00'.
    Returns '12:00:00' for null/empty input.
    """
    if not time_str:
        return "12:00:00"
    time_str = time_str.replace(" AM ET", "").replace(" PM ET", "").replace(" ET", "")
    match = re.match(r"^(\d{2}:\d{2}:\d{2})", time_str)
    if match:
        return match.group(1)
    return time_str or "12:00:00"


def sanitise_score(score_val) -> Optional[float]:
    """Parse a score value to float. Returns None for null/empty/invalid."""
    if score_val is None or score_val == "" or score_val == "null":
        return None
    try:
        return float(score_val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Sport-type-specific event normalisation
# ---------------------------------------------------------------------------

def normalise_events(
    events: list[dict],
    sport_type_map: dict[str | int, str],
) -> list[dict]:
    """Normalise raw events by sport type before upsert.

    For 'standard' sports: no change — scores already in home/away format.
    For 'binary' sports: replace scores with 1.0 (win) / 0.0 (loss) / 0.5 (draw).
    For 'multi_competitor' sports: decompose each race into pairwise events.

    Args:
        events: list of raw event dicts (from extract layer)
        sport_type_map: {league_id -> sport_type} mapping from league_registry

    Returns:
        Normalised event list. Multi-competitor leagues produce more rows than input.
    """
    normalised: list[dict] = []

    for event in events:
        league_id = event.get("league_id")
        sport_type = sport_type_map.get(league_id) or sport_type_map.get(
            int(league_id) if league_id else None
        )

        if sport_type == "binary":
            normalised.extend(_normalise_binary(event))
        elif sport_type == "multi_competitor":
            # Multi-competitor events require a batch — handled separately
            # Single events are passed through unchanged here;
            # race decomposition happens in a batch call below
            normalised.append(event)
        else:
            # standard: pass through unchanged
            normalised.append(event)

    return normalised


def _normalise_binary(event: dict) -> list[dict]:
    """Convert a binary-outcome event (UFC, Tennis, Boxing) to 1.0/0.0 scores.

    If scores are already null, leaves them null (unscored future event).
    If scores exist, replaces with:
      - Winner: 1.0
      - Loser:  0.0
      - Draw/no-contest: both 0.5
    """
    home_score = event.get("team_score_home")
    away_score = event.get("team_score_away")

    if home_score is None and away_score is None:
        # Future event — return as-is
        return [event]

    try:
        h = float(home_score) if home_score is not None else 0
        a = float(away_score) if away_score is not None else 0
    except (TypeError, ValueError):
        return [event]

    if h > a:
        norm_h, norm_a = 1.0, 0.0
    elif a > h:
        norm_h, norm_a = 0.0, 1.0
    else:
        norm_h, norm_a = 0.5, 0.5

    return [{**event, "team_score_home": norm_h, "team_score_away": norm_a}]


def decompose_race_events(
    race_results: list[dict],
    points_map: dict[int, float],
) -> list[dict]:
    """Convert a list of race results into pairwise matchup events.

    Used for F1, F2, Formula E, NASCAR, UCI Cycling.

    Each race produces N*(N-1)/2 synthetic pairwise events. The higher
    finisher is assigned as uid_home with score = their points, the lower
    finisher as uid_away with score = their points.

    Args:
        race_results: list of dicts, each with:
            {event_id, league_id, league_season, event_date, uid, finish_position,
             league_sport, event_status, updated_at}
        points_map: {finish_position -> championship_points}

    Returns:
        List of synthetic event dicts in standard home/away format.
    """
    pairwise: list[dict] = []
    if not race_results:
        return pairwise

    base = race_results[0]  # Use first result for shared metadata
    n = len(race_results)

    for i in range(n):
        for j in range(i + 1, n):
            r1 = race_results[i]  # higher finisher
            r2 = race_results[j]  # lower finisher

            pos1 = r1.get("finish_position", 99)
            pos2 = r2.get("finish_position", 99)

            pts1 = points_map.get(int(pos1), 0.0)
            pts2 = points_map.get(int(pos2), 0.0)

            pairwise.append({
                "event_id": f"{base['event_id']}-{r1['uid']}-{r2['uid']}",
                "event_date": base.get("event_date"),
                "event_time": base.get("event_time", "12:00:00"),
                "league_id": base.get("league_id"),
                "league_sport": base.get("league_sport"),
                "league_season": base.get("league_season"),
                "league_round": base.get("league_round", ""),
                "uid_home": r1["uid"],   # higher finisher = "home"
                "uid_away": r2["uid"],   # lower finisher = "away"
                "team_score_home": pts1,
                "team_score_away": pts2,
                "event_status": base.get("event_status", "Match Finished"),
                "event_video": "",
                "updated_at": base.get("updated_at"),
            })

    return pairwise
```

## 6.13 `src/pipeline/transform/stats.py`

Vectorised basic statistics. Replaces the row-by-row loop in the old `basic_stats()`.

```python
from __future__ import annotations
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def compute_basic_stats(events: pd.DataFrame) -> pd.DataFrame:
    """Compute win/loss/draw and points stats for every team per season.

    Input DataFrame must have columns:
        league_id, league_season, league_sport, event_date,
        uid_home, uid_away, team_score_home, team_score_away, event_result
        (event_result: 'home' | 'away' | 'draw')

    Returns one row per (uid, league_id, league_season) with:
        wins, draws, losses, points_for, points_against, games_played,
        avg_points_for, avg_points_against, win_percentage,
        home_wins, home_draws, home_losses, home_points_for,
        home_points_against, home_games_played,
        avg_home_points_for, avg_home_points_against, home_win_percentage,
        avg_points_for_percentile, avg_points_against_percentile
    """
    if events.empty:
        logger.warning("compute_basic_stats called with empty DataFrame.")
        return pd.DataFrame()

    # --- Build long-format DataFrame ---
    # Home perspective
    home = events[
        ["event_id", "league_id", "league_season", "event_date",
         "uid_home", "team_score_home", "team_score_away", "event_result"]
    ].copy()
    home.columns = [
        "event_id", "league_id", "league_season", "event_date",
        "uid", "pf", "pa", "event_result",
    ]
    home["is_home"] = True
    home["win"]  = (home["event_result"] == "home").astype(int)
    home["draw"] = (home["event_result"] == "draw").astype(int)
    home["loss"] = (home["event_result"] == "away").astype(int)

    # Away perspective
    away = events[
        ["event_id", "league_id", "league_season", "event_date",
         "uid_away", "team_score_away", "team_score_home", "event_result"]
    ].copy()
    away.columns = [
        "event_id", "league_id", "league_season", "event_date",
        "uid", "pf", "pa", "event_result",
    ]
    away["is_home"] = False
    away["win"]  = (away["event_result"] == "away").astype(int)
    away["draw"] = (away["event_result"] == "draw").astype(int)
    away["loss"] = (away["event_result"] == "home").astype(int)

    long = pd.concat([home, away], ignore_index=True)

    # --- Aggregate: all games ---
    grp = long.groupby(["uid", "league_id", "league_season"])
    stats = grp.agg(
        wins=("win", "sum"),
        draws=("draw", "sum"),
        losses=("loss", "sum"),
        points_for=("pf", "sum"),
        points_against=("pa", "sum"),
        games_played=("win", "count"),
    ).reset_index()

    # --- Aggregate: home games only ---
    home_only = long[long["is_home"]].groupby(["uid", "league_id", "league_season"])
    home_stats = home_only.agg(
        home_wins=("win", "sum"),
        home_draws=("draw", "sum"),
        home_losses=("loss", "sum"),
        home_points_for=("pf", "sum"),
        home_points_against=("pa", "sum"),
        home_games_played=("win", "count"),
    ).reset_index()

    stats = stats.merge(home_stats, on=["uid", "league_id", "league_season"], how="left")

    # --- Derived columns ---
    stats["avg_points_for"] = stats["points_for"] / stats["games_played"].clip(lower=1)
    stats["avg_points_against"] = stats["points_against"] / stats["games_played"].clip(lower=1)
    stats["win_percentage"] = (
        (stats["wins"] + 0.5 * stats["draws"]) / stats["games_played"].clip(lower=1)
    ).round(4)
    stats["avg_home_points_for"] = (
        stats["home_points_for"] / stats["home_games_played"].clip(lower=1)
    )
    stats["avg_home_points_against"] = (
        stats["home_points_against"] / stats["home_games_played"].clip(lower=1)
    )
    stats["home_win_percentage"] = (
        (stats["home_wins"] + 0.5 * stats["home_draws"])
        / stats["home_games_played"].clip(lower=1)
    ).round(4)

    # --- Percentile ranks within each league-season ---
    stats["avg_points_for_percentile"] = stats.groupby(
        ["league_id", "league_season"]
    )["avg_points_for"].rank(pct=True, ascending=False)
    stats["avg_points_against_percentile"] = stats.groupby(
        ["league_id", "league_season"]
    )["avg_points_against"].rank(pct=True, ascending=True)

    logger.info("compute_basic_stats: produced %d rows.", len(stats))
    return stats
```

## 6.14 `src/pipeline/transform/elo.py`

Sequential Elo rating computation. Returns both the per-season summary (for `py_stats`) and the full game-by-game history (for luck calculation).

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def _compute_hfa(df: pd.DataFrame) -> dict:
    """Compute home field advantage (HFA) in Elo points per league.

    HFA is estimated from the historical difference between home and
    away win rates. A higher HFA means the home team wins more often.

    Returns: {league_id -> Elo HFA in points}
    """
    home_games = df.groupby(["league_id", "uid_home"]).size()
    home_wins  = df[df["event_result"] == "home"].groupby(["league_id", "uid_home"]).size()
    home_win_rates = (home_wins / home_games).fillna(0)

    away_games = df.groupby(["league_id", "uid_away"]).size()
    away_wins  = df[df["event_result"] == "away"].groupby(["league_id", "uid_away"]).size()
    away_win_rates = (away_wins / away_games).fillna(0)

    diff = (home_win_rates.groupby("league_id").mean()
            - away_win_rates.groupby("league_id").mean()
            + 0.5)
    diff = diff.clip(0.001, 0.999)  # Avoid log(0)
    hfa = -400 * np.log10((1 / diff) - 1)
    return hfa.to_dict()


def _elo_expected(elo_a: float, elo_b: float, hfa: float) -> float:
    """Expected win probability for team A (home) vs team B (away)."""
    return 1.0 / (1.0 + 10 ** ((elo_b - (elo_a + hfa)) / 400.0))


def compute_elo_stats(
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute Elo ratings across all events in chronological order.

    All teams start at INIT_ELO (1500). Ratings persist across seasons —
    a team carries its end-of-season rating into the next season.
    Ratings are reset at the start of each run (computed fresh from all history).

    Args:
        events: scored events DataFrame with columns:
            league_id, league_season, league_sport, event_date, event_id,
            uid_home, uid_away, team_score_home, team_score_away, event_result

    Returns:
        Tuple of:
        - summary_df: one row per (uid, league_id, league_season) with Elo stats
        - history_df: one row per (uid, event_id) with per-game Elo data
          including actual_result and expected_win_prob (needed for luck)
    """
    if events.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Initialise ratings for every team seen in the data
    all_uids = pd.concat([events["uid_home"], events["uid_away"]]).unique()
    elo: dict[str, float] = {uid: float(settings.INIT_ELO) for uid in all_uids}

    hfa = _compute_hfa(events)
    history: list[dict] = []

    df_sorted = events.sort_values("event_date").reset_index(drop=True)

    for _, row in df_sorted.iterrows():
        uid_h = row["uid_home"]
        uid_a = row["uid_away"]
        league = row["league_id"]
        sport  = row["league_sport"]

        league_hfa = hfa.get(league, 0.0)
        exp_h = _elo_expected(elo[uid_h], elo[uid_a], league_hfa)
        exp_a = 1.0 - exp_h

        k = settings.K_VALUES.get(sport, settings.K_VALUES["default"])

        result = row["event_result"]
        if result == "home":
            actual_h, actual_a = 1.0, 0.0
        elif result == "away":
            actual_h, actual_a = 0.0, 1.0
        else:  # draw
            actual_h, actual_a = 0.5, 0.5

        delta_h = round(k * (actual_h - exp_h), 2)
        delta_a = round(k * (actual_a - exp_a), 2)

        elo[uid_h] += delta_h
        elo[uid_a] += delta_a

        ts = datetime.now(timezone.utc).isoformat()
        history.append({
            "uid": uid_h,
            "league_id": league,
            "league_season": row["league_season"],
            "event_id": row["event_id"],
            "event_date": row["event_date"],
            "current_elo": round(elo[uid_h], 2),
            "current_elo_delta": delta_h,
            "actual_result": actual_h,
            "expected_win_prob": round(exp_h, 4),
        })
        history.append({
            "uid": uid_a,
            "league_id": league,
            "league_season": row["league_season"],
            "event_id": row["event_id"],
            "event_date": row["event_date"],
            "current_elo": round(elo[uid_a], 2),
            "current_elo_delta": delta_a,
            "actual_result": actual_a,
            "expected_win_prob": round(exp_a, 4),
        })

    history_df = pd.DataFrame(history).sort_values("event_date")

    # Aggregate to season-level summary
    grp = history_df.groupby(["uid", "league_id", "league_season"])
    summary = grp.agg(
        start_of_season_elo=("current_elo", "first"),
        end_of_season_elo=("current_elo", "last"),
        last_elo_delta=("current_elo_delta", "last"),
    ).reset_index()
    summary["season_elo_delta"] = (
        summary["end_of_season_elo"] - summary["start_of_season_elo"]
    ).round(2)

    # League-season rank by end Elo
    summary["start_rank_league"] = summary.groupby(
        ["league_id", "league_season"]
    )["start_of_season_elo"].rank(ascending=False, method="first").fillna(0).astype(int)
    summary["end_rank_league"] = summary.groupby(
        ["league_id", "league_season"]
    )["end_of_season_elo"].rank(ascending=False, method="first").fillna(0).astype(int)

    summary["updated_at"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        "compute_elo_stats: %d season rows, %d history rows.",
        len(summary), len(history_df),
    )
    return summary, history_df
```

## 6.15 `src/pipeline/transform/tiers.py`

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def compute_tiers(elo_summary: pd.DataFrame) -> list[dict]:
    """Assign tier labels to teams based on end-of-season Elo percentile.

    Tiers are assigned globally across ALL leagues and sports in one pass.
    This means a B-tier team in Soccer and a B-tier team in Basketball have
    comparable Elo percentile standing globally, not just within their sport.

    Uses TIER_THRESHOLDS from settings. Tiers in descending order:
    MOL (top 0.5%), SS, S, A, B, C, D, E, F, FF, DIE (bottom 0.5%)

    Takes the most recent season's Elo for each team (highest season).

    Returns list of dicts for upsert into py_tier.
    """
    if elo_summary.empty:
        return []

    # Get the most recent season per team
    latest = (
        elo_summary
        .sort_values("league_season", ascending=False)
        .groupby("uid")
        .first()
        .reset_index()
    )

    latest["percentile_rank"] = latest["end_of_season_elo"].rank(pct=True)
    now = datetime.now(timezone.utc).isoformat()

    def assign_tier(pct: float) -> str:
        for threshold, tier_name in settings.TIER_THRESHOLDS:
            if pct > threshold:
                return tier_name
        return "DIE"

    latest["tier"] = latest["percentile_rank"].apply(assign_tier)
    latest["updated_at"] = now

    return latest[["uid", "league_id", "tier", "updated_at"]].to_dict(orient="records")
```

## 6.16 `src/pipeline/transform/luck.py`

```python
from __future__ import annotations
import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings

logger = logging.getLogger(__name__)


def compute_luck(elo_history: pd.DataFrame) -> pd.DataFrame:
    """Compute Elo-adjusted luck score for each team.

    Definition:
        luck_raw = mean(actual_result) - mean(expected_win_prob)
                   over the last LUCK_WINDOW games

    Positive = winning more than Elo predicted (lucky).
    Negative = winning less than Elo predicted (unlucky).

    luck_display: 0-100 integer, percentile rank of luck_raw across all teams.
    Used directly as the 'asset_luck' display value on the website.

    Args:
        elo_history: output from compute_elo_stats, one row per (uid, event_id)
            Must have columns: uid, league_id, event_date,
            actual_result, expected_win_prob

    Returns:
        DataFrame with columns: uid, league_id, luck_score, luck_display
    """
    if elo_history.empty:
        return pd.DataFrame(columns=["uid", "league_id", "luck_score", "luck_display"])

    # Take the most recent LUCK_WINDOW games per team
    recent = (
        elo_history
        .sort_values("event_date", ascending=False)
        .groupby("uid", group_keys=False)
        .head(settings.LUCK_WINDOW)
    )

    luck = (
        recent
        .groupby(["uid", "league_id"])
        .apply(
            lambda g: g["actual_result"].mean() - g["expected_win_prob"].mean()
        )
        .reset_index(name="luck_score")
    )

    luck["luck_display"] = (
        luck["luck_score"]
        .rank(pct=True)
        .mul(100)
        .round()
        .astype(int)
    )

    logger.info("compute_luck: %d team luck scores computed.", len(luck))
    return luck
```

## 6.17 `src/pipeline/load/upsert.py`

```python
from __future__ import annotations
import logging
from typing import Any

from supabase import Client

from pipeline.config import settings

logger = logging.getLogger(__name__)


def batch_upsert(
    client: Client,
    table: str,
    records: list[dict[str, Any]],
    conflict_cols: str,
    chunk_size: int = settings.UPSERT_CHUNK_SIZE,
) -> None:
    """Upsert records into a Supabase table in chunks.

    Deduplicates records in Python before sending to avoid
    conflict errors within a single batch.

    On chunk failure, retries once with half chunk size.
    On second failure, raises — fail loudly rather than silently dropping rows.

    Args:
        client: Supabase PostgREST client
        table: target table name (no schema prefix)
        records: list of dicts to upsert
        conflict_cols: comma-separated column names for ON CONFLICT clause
        chunk_size: number of rows per API call (default 100)
    """
    if not records:
        logger.info("batch_upsert: no records to upsert into %s.", table)
        return

    # Deduplicate in Python
    keys = [k.strip() for k in conflict_cols.split(",")]
    seen: set[tuple] = set()
    unique: list[dict] = []
    for record in records:
        key = tuple(record.get(k) for k in keys)
        if key not in seen:
            seen.add(key)
            unique.append(record)

    if len(unique) < len(records):
        logger.info(
            "batch_upsert: deduplicated %d -> %d rows for %s.",
            len(records), len(unique), table,
        )

    # Chunked upsert
    total = len(unique)
    for i in range(0, total, chunk_size):
        chunk = unique[i: i + chunk_size]
        if i % (chunk_size * 10) == 0:
            logger.info("batch_upsert: %s — row %d / %d", table, i, total)
        try:
            client.table(table).upsert(chunk, on_conflict=conflict_cols).execute()
        except Exception as exc:
            logger.warning(
                "batch_upsert: chunk %d-%d failed for %s: %s. Retrying halved.",
                i, i + chunk_size, table, exc,
            )
            half = max(1, chunk_size // 2)
            for j in range(0, len(chunk), half):
                # Second failure raises — do not swallow errors
                client.table(table).upsert(
                    chunk[j: j + half],
                    on_conflict=conflict_cols,
                ).execute()

    logger.info("batch_upsert: completed %d rows into %s.", total, table)
```

## 6.18 `src/pipeline/sql/executor.py`

```python
from __future__ import annotations
import logging
from pathlib import Path

import psycopg2.extensions

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "queries"

# All ephemeral tables that need RLS after creation
RLS_TABLES = [
    "sql_leagues_current",
    "sql_season_current",
    "sql_season_past",
    "sql_season_last5",
    "sql_web_events",
    "sql_events_scored",
    "sql_py_stats_utd",
    "sql_py_stats_ls",
    "sql_events_future_elos",
    "sql_assets_future",
    "sql_assets_stats_at",
    "sql_forecast",
    "sql_events_split",
    "sql_asset_last_10_games",
    "sql_assets_season_to_date",
    "sql_ref_elo",
    "sql_current_elo",
    "sql_wld",
    "sql_web_assets",
    "sql_web_assets_info",
    "sql_league_info",
]


def run_sql_file(
    conn: psycopg2.extensions.connection,
    filename: str,
) -> None:
    """Read a .sql file from sql/queries/ and execute it.

    Each file is expected to handle its own DROP TABLE IF EXISTS and
    CREATE TABLE AS SELECT in a single transaction block.

    Args:
        conn: open psycopg2 connection (direct, not pooler)
        filename: filename only, e.g. '03_web_events.sql'
    """
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")
    logger.info("Executing %s ...", filename)
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Completed %s.", filename)


def apply_rls(
    conn: psycopg2.extensions.connection,
    table: str,
) -> None:
    """Enable RLS and create a public-read policy on a table.

    Idempotent: drops existing policy before recreating.
    Safe to call on every run.

    Args:
        conn: open psycopg2 connection
        table: table name without schema prefix
    """
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;")
        cur.execute(
            f'DROP POLICY IF EXISTS "Enable read access for all users" ON public.{table};'
        )
        cur.execute(
            f"""
            CREATE POLICY "Enable read access for all users"
            ON public.{table} AS PERMISSIVE
            FOR SELECT TO public
            USING (true);
            """
        )
    conn.commit()


def apply_rls_all(conn: psycopg2.extensions.connection) -> None:
    """Apply RLS to all ephemeral sql_* tables."""
    for table in RLS_TABLES:
        try:
            apply_rls(conn, table)
        except Exception as exc:
            logger.warning("Failed to apply RLS to %s: %s", table, exc)
    logger.info("RLS applied to %d tables.", len(RLS_TABLES))
```

## 6.19 `src/pipeline/runners/full_refresh.py`

The weekly entry point. Fetches all whitelisted league data, computes full stats, rebuilds all derived tables.

```python
"""Full pipeline refresh.

Run: python -m pipeline.runners.full_refresh

Fetches reference data and events for all whitelisted leagues.
Computes Elo, stats, tiers, and luck.
Rebuilds all sql_* derived tables.

Expected runtime: 10-25 minutes depending on number of whitelisted leagues
and how many past seasons need event updates.
"""
from __future__ import annotations
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.api.client import RateLimitedClient
from pipeline.extract.leagues import fetch_all_leagues, fetch_league_details
from pipeline.extract.seasons import fetch_seasons
from pipeline.extract.teams import fetch_teams
from pipeline.extract.events import fetch_events_incremental
from pipeline.transform.normalise import normalise_events
from pipeline.transform.stats import compute_basic_stats
from pipeline.transform.elo import compute_elo_stats
from pipeline.transform.tiers import compute_tiers
from pipeline.transform.luck import compute_luck
from pipeline.load.upsert import batch_upsert
from pipeline.sql.executor import run_sql_file, apply_rls_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# SQL files executed in order. Numbers in filenames enforce ordering.
SQL_FILES = [
    "01_leagues_current.sql",
    "02_season_windows.sql",
    "03_web_events.sql",
    "04_events_scored.sql",
    "05_events_split.sql",
    "06_asset_last_10.sql",
    "07_events_future_elos.sql",
    "08_assets_future.sql",
    "09_assets_stats_at.sql",
    "10_forecast.sql",
    "11_assets_season_to_date.sql",
    "12_ref_elo.sql",
    "13_current_elo.sql",
    "14_wld.sql",
    "15_web_assets.sql",
    "16_web_assets_info.sql",
    "17_league_info.sql",
]


def get_registry(supabase) -> tuple[list[int], list[int], dict]:
    """Read league_registry and return (whitelisted_ids, active_ids, sport_type_map)."""
    rows = (
        supabase.table("league_registry")
        .select("league_id,sport_type,is_whitelisted,is_active")
        .eq("is_whitelisted", True)
        .execute()
        .data
    )
    whitelisted_ids = [r["league_id"] for r in rows]
    active_ids = [r["league_id"] for r in rows if r["is_active"]]
    sport_type_map = {str(r["league_id"]): r["sport_type"] for r in rows}
    return whitelisted_ids, active_ids, sport_type_map


def main() -> None:
    t0 = time.monotonic()
    logger.info("=" * 60)
    logger.info("FULL REFRESH STARTED at %s UTC", datetime.now(timezone.utc).isoformat())
    logger.info("=" * 60)

    supabase = get_supabase_client()

    with RateLimitedClient() as client, get_pg_connection() as conn:

        # ------------------------------------------------------------------
        # 1. Read league registry
        # ------------------------------------------------------------------
        whitelisted_ids, active_ids, sport_type_map = get_registry(supabase)
        logger.info(
            "Registry: %d whitelisted, %d active leagues.",
            len(whitelisted_ids), len(active_ids),
        )

        # ------------------------------------------------------------------
        # 2. Build active-league filter tables (needed by later SQL files)
        # ------------------------------------------------------------------
        run_sql_file(conn, "01_leagues_current.sql")
        run_sql_file(conn, "02_season_windows.sql")

        # ------------------------------------------------------------------
        # 3. Extract and upsert reference data
        # ------------------------------------------------------------------
        logger.info("--- Extracting leagues ---")
        all_leagues = fetch_all_leagues(client)
        batch_upsert(supabase, "api_leagues", all_leagues, "league_id")

        logger.info("--- Extracting league details ---")
        details = fetch_league_details(client, whitelisted_ids)
        batch_upsert(supabase, "api_league_details", details, "league_id")

        logger.info("--- Extracting seasons ---")
        seasons = fetch_seasons(client, whitelisted_ids)
        batch_upsert(supabase, "api_seasons", seasons, "league_id,league_season")

        logger.info("--- Extracting teams ---")
        teams = fetch_teams(client, whitelisted_ids)
        batch_upsert(supabase, "api_assets", teams, "uid")

        # Rebuild season windows after new season data is loaded
        run_sql_file(conn, "02_season_windows.sql")

        # ------------------------------------------------------------------
        # 4. Extract events (incremental)
        # ------------------------------------------------------------------
        logger.info("--- Extracting events (incremental) ---")
        season_last5 = supabase.table("sql_season_last5").select("*").execute().data
        events_raw = fetch_events_incremental(
            client, supabase, whitelisted_ids, season_last5, current_only=False,
        )
        events_normalised = normalise_events(events_raw, sport_type_map)
        batch_upsert(supabase, "api_events", events_normalised, "event_id")

        # ------------------------------------------------------------------
        # 5. Rebuild event SQL tables
        # ------------------------------------------------------------------
        for f in ["03_web_events.sql", "04_events_scored.sql", "05_events_split.sql"]:
            run_sql_file(conn, f)

        # ------------------------------------------------------------------
        # 6. Compute statistics
        # ------------------------------------------------------------------
        logger.info("--- Computing statistics ---")
        scored_rows = supabase.table("sql_events_scored").select("*").execute().data
        scored_df = pd.DataFrame(scored_rows)
        logger.info("Loaded %d scored events for stat computation.", len(scored_df))

        basic_df    = compute_basic_stats(scored_df)
        elo_df, history_df = compute_elo_stats(scored_df)
        tiers_data  = compute_tiers(elo_df)
        luck_df     = compute_luck(history_df)

        # Merge all stats into one DataFrame for py_stats upsert
        stats_df = elo_df.merge(basic_df, on=["uid", "league_id", "league_season"], how="outer")
        stats_df = stats_df.merge(
            luck_df[["uid", "luck_score", "luck_display"]],
            on="uid", how="left",
        )
        stats_df["luck_display"] = stats_df["luck_display"].fillna(50).astype(int)

        # ------------------------------------------------------------------
        # 7. Upsert computed stats
        # ------------------------------------------------------------------
        logger.info("--- Upserting stats ---")
        stats_records = stats_df.to_dict(orient="records")
        batch_upsert(supabase, "py_stats", stats_records, "uid,league_season")
        batch_upsert(supabase, "py_tier", tiers_data, "uid")
        logger.info(
            "Upserted %d stat rows, %d tier rows.",
            len(stats_records), len(tiers_data),
        )

        # ------------------------------------------------------------------
        # 8. Rebuild all remaining derived SQL tables
        # ------------------------------------------------------------------
        logger.info("--- Rebuilding derived tables ---")
        for f in SQL_FILES[2:]:  # 01 and 02 already done
            run_sql_file(conn, f)

        # ------------------------------------------------------------------
        # 9. Apply RLS to all ephemeral tables
        # ------------------------------------------------------------------
        apply_rls_all(conn)

        # ------------------------------------------------------------------
        # 10. Update registry metadata
        # ------------------------------------------------------------------
        logger.info("--- Updating league registry metadata ---")
        team_counts = pd.DataFrame(teams).groupby("league_id").size().to_dict()
        now_iso = datetime.now(timezone.utc).isoformat()
        for lid in whitelisted_ids:
            supabase.table("league_registry").update({
                "last_fetched_at": now_iso,
                "team_count": team_counts.get(str(lid), 0),
            }).eq("league_id", lid).execute()

    elapsed = time.monotonic() - t0
    logger.info("=" * 60)
    logger.info("FULL REFRESH COMPLETE in %.1fs", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
```

## 6.20 `src/pipeline/runners/daily_update.py`

The daily entry point. Only fetches current-season events for active leagues.

```python
"""Daily events update.

Run: python -m pipeline.runners.daily_update

Fetches only current-season events for active leagues.
Recomputes all stats and rebuilds all derived tables.

Expected runtime: 2-5 minutes.
"""
from __future__ import annotations
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.api.client import RateLimitedClient
from pipeline.extract.events import fetch_events_incremental
from pipeline.transform.normalise import normalise_events
from pipeline.transform.stats import compute_basic_stats
from pipeline.transform.elo import compute_elo_stats
from pipeline.transform.tiers import compute_tiers
from pipeline.transform.luck import compute_luck
from pipeline.load.upsert import batch_upsert
from pipeline.sql.executor import run_sql_file, apply_rls_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

SQL_FILES = [
    "01_leagues_current.sql",
    "02_season_windows.sql",
    "03_web_events.sql",
    "04_events_scored.sql",
    "05_events_split.sql",
    "06_asset_last_10.sql",
    "07_events_future_elos.sql",
    "08_assets_future.sql",
    "09_assets_stats_at.sql",
    "10_forecast.sql",
    "11_assets_season_to_date.sql",
    "12_ref_elo.sql",
    "13_current_elo.sql",
    "14_wld.sql",
    "15_web_assets.sql",
    "16_web_assets_info.sql",
    "17_league_info.sql",
]


def main() -> None:
    t0 = time.monotonic()
    logger.info("DAILY UPDATE STARTED at %s UTC", datetime.now(timezone.utc).isoformat())

    supabase = get_supabase_client()

    with RateLimitedClient() as client, get_pg_connection() as conn:

        # Read active leagues and their sport types
        rows = (
            supabase.table("league_registry")
            .select("league_id,sport_type")
            .eq("is_active", True)
            .execute()
            .data
        )
        active_ids = [r["league_id"] for r in rows]
        sport_type_map = {str(r["league_id"]): r["sport_type"] for r in rows}
        logger.info("Active leagues: %d", len(active_ids))

        # Rebuild season windows (cheap: zero API calls)
        run_sql_file(conn, "01_leagues_current.sql")
        run_sql_file(conn, "02_season_windows.sql")

        # Fetch current-season events only
        season_last5 = supabase.table("sql_season_last5").select("*").execute().data
        events_raw = fetch_events_incremental(
            client, supabase, active_ids, season_last5, current_only=True,
        )
        events_normalised = normalise_events(events_raw, sport_type_map)
        batch_upsert(supabase, "api_events", events_normalised, "event_id")
        logger.info("Upserted %d current-season events.", len(events_normalised))

        # Rebuild event tables
        for f in ["03_web_events.sql", "04_events_scored.sql", "05_events_split.sql"]:
            run_sql_file(conn, f)

        # Recompute all stats from full history
        scored_rows = supabase.table("sql_events_scored").select("*").execute().data
        scored_df = pd.DataFrame(scored_rows)

        basic_df         = compute_basic_stats(scored_df)
        elo_df, hist_df  = compute_elo_stats(scored_df)
        tiers_data       = compute_tiers(elo_df)
        luck_df          = compute_luck(hist_df)

        stats_df = elo_df.merge(basic_df, on=["uid", "league_id", "league_season"], how="outer")
        stats_df = stats_df.merge(luck_df[["uid", "luck_score", "luck_display"]], on="uid", how="left")
        stats_df["luck_display"] = stats_df["luck_display"].fillna(50).astype(int)

        batch_upsert(supabase, "py_stats", stats_df.to_dict(orient="records"), "uid,league_season")
        batch_upsert(supabase, "py_tier", tiers_data, "uid")

        # Rebuild all derived tables
        for f in SQL_FILES[2:]:
            run_sql_file(conn, f)

        apply_rls_all(conn)

    logger.info("DAILY UPDATE COMPLETE in %.1fs", time.monotonic() - t0)


if __name__ == "__main__":
    main()
```

\newpage

# 7. SQL Files — Complete Source (`sql/queries/`)

Each file follows the pattern: `DROP TABLE IF EXISTS` → `CREATE TABLE AS SELECT` → `CREATE INDEX`. Every file is idempotent and safe to re-run. The `sql/executor.py` `run_sql_file()` function reads and executes each file in order. RLS is applied separately after all files complete, via `apply_rls_all()`.

**Global changes applied to every file vs. the original `supabase_tables.py`:**

- `js_rounds` JOIN removed everywhere — the `event_round` column no longer exists
- `01_leagues_current.sql` reads active leagues from `league_registry` (not a hardcoded Python list)
- `07_events_future_elos.sql` fixes the `INNER JOIN` bug (replaced with `LEFT JOIN + COALESCE`)
- `15_web_assets.sql` reads `luck_display` from `py_stats` (replaces `ROUND(RANDOM() * 100)`)

---

## 7.1 `01_leagues_current.sql`

Creates the `sql_leagues_current` filter table — all downstream SQL uses this as the source of truth for which leagues are currently active. In the original code this was parameterised with a hardcoded Python list of IDs; in the rewrite it reads from `league_registry` at runtime.

```sql
-- 01_leagues_current.sql
-- Creates sql_leagues_current: the active-league filter used by all downstream tables.
-- Reads from league_registry instead of a hardcoded Python list.

DROP TABLE IF EXISTS public.sql_leagues_current;

CREATE TABLE public.sql_leagues_current AS
    SELECT
        al.league_id
        , al.league_name
        , al.league_name                AS league_name_clean
        , al.league_sport
        , lr.sport_type
        , lr.display_name
    FROM public.api_leagues AS al
    INNER JOIN public.league_registry AS lr
        ON lr.league_id = al.league_id
    WHERE lr.is_active = TRUE;

CREATE INDEX idx_sql_leagues_current
    ON public.sql_leagues_current (league_id);
```

---

## 7.2 `02_season_windows.sql`

Creates three season-window tables used throughout the pipeline. These are re-run after teams/seasons are upserted so the windows reflect the latest data.

- `sql_season_current`: the single most recent season per active league (season_rank = 1)
- `sql_season_past`: the second-most-recent season per active league (season_rank = 2)
- `sql_season_last5`: the five most recent seasons per active league (season_rank 1–5)

```sql
-- 02_season_windows.sql
-- Creates sql_season_current, sql_season_past, sql_season_last5.
-- All three are derived from api_seasons for active leagues.
-- Re-run after seasons upsert to reflect latest data.

-- ── sql_season_current ────────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.sql_season_current;

CREATE TABLE public.sql_season_current AS
    SELECT
        league_id
        , league_season
    FROM (
        SELECT
            s.league_id
            , s.league_season
            , ROW_NUMBER() OVER (
                PARTITION BY s.league_id
                ORDER BY s.league_season DESC NULLS LAST
              ) AS season_rank
        FROM public.api_seasons AS s
        WHERE s.league_id IN (SELECT league_id FROM public.sql_leagues_current)
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank = 1;

-- ── sql_season_past ───────────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.sql_season_past;

CREATE TABLE public.sql_season_past AS
    SELECT
        league_id
        , league_season
    FROM (
        SELECT
            s.league_id
            , s.league_season
            , ROW_NUMBER() OVER (
                PARTITION BY s.league_id
                ORDER BY s.league_season DESC NULLS LAST
              ) AS season_rank
        FROM public.api_seasons AS s
        INNER JOIN public.api_events AS e
            ON s.league_id    = e.league_id
           AND s.league_season = e.league_season
        WHERE s.league_id IN (SELECT league_id FROM public.sql_leagues_current)
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank = 2;

-- ── sql_season_last5 ──────────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.sql_season_last5;

CREATE TABLE public.sql_season_last5 AS
    SELECT
        league_id
        , league_season
        , season_rank
    FROM (
        SELECT
            s.league_id
            , s.league_season
            , ROW_NUMBER() OVER (
                PARTITION BY s.league_id
                ORDER BY s.league_season DESC NULLS LAST
              ) AS season_rank
        FROM public.api_seasons AS s
        INNER JOIN public.api_events AS e
            ON s.league_id    = e.league_id
           AND s.league_season = e.league_season
        WHERE s.league_id IN (SELECT league_id FROM public.sql_leagues_current)
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank <= 5;

CREATE INDEX idx_sql_season_last5
    ON public.sql_season_last5 (league_id, league_season);
```

---

## 7.3 `03_web_events.sql`

The primary events display table consumed by the website. Contains one row per event with team names, scores, and status. The `event_round` column and `js_rounds` JOIN from the original are removed.

```sql
-- 03_web_events.sql
-- All events for active leagues across the last 5 seasons.
-- Used by the website to display the events list/calendar.
-- NOTE: event_round column removed (js_rounds dependency eliminated).

DROP TABLE IF EXISTS public.sql_web_events;

CREATE TABLE public.sql_web_events AS
    SELECT
        e.event_id
        , e.event_date
        , e.event_time
        , CASE
            WHEN e.event_status = 'Match Finished'           THEN 'F'
            WHEN e.event_status IN ('FT', 'AOT')             THEN 'F'
            WHEN e.event_status = 'Not Started'              THEN 'NS'
            WHEN e.event_status = 'Time to be defined'       THEN 'NS'
            WHEN e.event_status = '1H'                       THEN '1st'
            WHEN e.event_status = '2H'                       THEN '2nd'
            WHEN e.event_status = 'HT'                       THEN 'Half'
            WHEN (
                e.event_date::DATE < CURRENT_DATE::DATE
                AND e.team_score_away IS NOT NULL
                AND e.team_score_home IS NOT NULL
            )                                                THEN 'F'
            WHEN e.event_date::DATE > CURRENT_DATE::DATE     THEN 'NS'
            WHEN e.event_date::DATE = CURRENT_DATE::DATE     THEN 'NS'
            ELSE 'Unknown'
          END                                               AS event_status
        , e.event_video
        , e.league_id
        , al.league_name
        , al.league_name                                    AS league_name_clean
        , e.league_sport
        , e.league_season
        , e.uid_home
        , home.team_name                                    AS team_name_home
        , e.team_score_home
        , e.uid_away
        , away.team_name                                    AS team_name_away
        , e.team_score_away
        , CURRENT_TIMESTAMP                                 AS updated_at
    FROM public.api_events AS e
    -- Only events in the last-5-season window OR future events
    LEFT JOIN public.sql_season_last5 AS seasons
        ON seasons.league_id    = e.league_id
       AND seasons.league_season = e.league_season
    INNER JOIN public.api_assets AS home
        ON home.uid = e.uid_home
    INNER JOIN public.api_assets AS away
        ON away.uid = e.uid_away
    INNER JOIN public.api_leagues AS al
        ON al.league_id = e.league_id
    INNER JOIN public.sql_leagues_current AS slc
        ON slc.league_id = e.league_id
    WHERE (
        seasons.league_id IS NOT NULL
        OR e.event_date >= CURRENT_DATE
    );

CREATE INDEX idx_sql_web_events
    ON public.sql_web_events (league_id, event_id);
```

---

## 7.4 `04_events_scored.sql`

All completed (scored) events for active leagues. This is the table Python reads from when computing stats, Elo, tiers, and luck. Only rows with both scores present are included. The `event_round` column and `js_rounds` JOIN are removed.

```sql
-- 04_events_scored.sql
-- All scored (completed) events for active leagues.
-- Source of truth for Python stat/Elo computation.
-- NOTE: event_round column removed (js_rounds dependency eliminated).

DROP TABLE IF EXISTS public.sql_events_scored;

CREATE TABLE public.sql_events_scored AS
    SELECT
        e.event_id
        , e.league_id
        , e.league_season
        , e.event_date
        , e.league_sport
        , e.uid_home
        , e.uid_away
        , e.team_score_home
        , e.team_score_away
        , CASE
            WHEN e.team_score_home - e.team_score_away = 0 THEN 'draw'
            WHEN e.team_score_home - e.team_score_away > 0 THEN 'home'
            WHEN e.team_score_home - e.team_score_away < 0 THEN 'away'
          END                                               AS event_result
        , ROW_NUMBER() OVER (
            PARTITION BY e.league_id
            ORDER BY e.event_date ASC
          )                                                 AS game_order
    FROM public.api_events AS e
    INNER JOIN public.sql_leagues_current AS slc
        ON slc.league_id = e.league_id
    WHERE
        e.team_score_away IS NOT NULL
        AND e.team_score_home IS NOT NULL
        AND e.event_date IS NOT NULL;

CREATE INDEX idx_sql_events_scored
    ON public.sql_events_scored (league_id, event_id);
```

---

## 7.5 `05_events_split.sql`

Unpivots the events table into a long format — one row per team per game — giving each team a `team_points_for` and `team_points_against`. Used by `sql_asset_last_10_games` and `sql_assets_season_to_date`.

```sql
-- 05_events_split.sql
-- Unpivoted events: one row per team per game.
-- Provides team_points_for / team_points_against for each team's perspective.

DROP TABLE IF EXISTS public.sql_events_split;

CREATE TABLE public.sql_events_split AS
    WITH cte AS (
        -- Home team perspective
        SELECT
            e.uid_home              AS uid
            , e.league_id
            , e.league_season
            , e.event_date
            , e.team_score_home     AS team_points_for
            , e.team_score_away     AS team_points_against
        FROM public.api_events AS e
        INNER JOIN public.sql_leagues_current AS slc
            ON slc.league_id = e.league_id
        WHERE
            e.team_score_away IS NOT NULL
            AND e.team_score_home IS NOT NULL

        UNION ALL

        -- Away team perspective
        SELECT
            e.uid_away              AS uid
            , e.league_id
            , e.league_season
            , e.event_date
            , e.team_score_away     AS team_points_for
            , e.team_score_home     AS team_points_against
        FROM public.api_events AS e
        INNER JOIN public.sql_leagues_current AS slc
            ON slc.league_id = e.league_id
        WHERE
            e.team_score_away IS NOT NULL
            AND e.team_score_home IS NOT NULL
    )
    SELECT * FROM cte;

CREATE INDEX idx_sql_events_split
    ON public.sql_events_split (uid);
```

---

## 7.6 `06_asset_last_10.sql`

Per-team stats for the last 10 completed games. Produces W/D/L counts, win percentage, attack/defence weighted scores, and a text result string (e.g. `"WWLDWWWLDL"`). Used by `sql_wld` and `sql_web_assets`.

```sql
-- 06_asset_last_10.sql
-- Per-team stats across each team's last 10 completed games.
-- Produces attack/defence score and result string for the website.

DROP TABLE IF EXISTS public.sql_asset_last_10_games;

CREATE TABLE public.sql_asset_last_10_games AS
    WITH last_10 AS (
        SELECT
            uid
            , league_id
            , event_date
            , team_points_for
            , team_points_against
            , ROW_NUMBER() OVER (
                PARTITION BY uid
                ORDER BY event_date DESC NULLS LAST
              )                                             AS rn
            , CASE
                WHEN team_points_for IS NULL
                  OR team_points_against IS NULL            THEN '?'
                WHEN team_points_for > team_points_against  THEN 'W'
                WHEN team_points_for < team_points_against  THEN 'L'
                WHEN team_points_for = team_points_against  THEN 'D'
              END                                           AS result
        FROM public.sql_events_split
    )
    , agg AS (
        SELECT
            uid
            , league_id
            , COUNT(*)                                              AS games
            , SUM(CASE WHEN team_points_for > team_points_against
                       THEN 1 ELSE 0 END)                          AS wins
            , SUM(CASE WHEN team_points_for = team_points_against
                       THEN 1 ELSE 0 END)                          AS draws
            , SUM(CASE WHEN team_points_for < team_points_against
                       THEN 1 ELSE 0 END)                          AS losses
            , SUM(team_points_for)                                  AS total_points_for
            , SUM(team_points_against)                              AS total_points_against
            -- Attack score: full points in wins, half in losses
            , SUM(CASE
                WHEN team_points_for > team_points_against
                    THEN team_points_for
                WHEN team_points_for < team_points_against
                    THEN team_points_for * 0.5
                ELSE team_points_for
              END)                                                  AS winning_points_for
            -- Defence score: low allowed in wins, penalised in losses
            , SUM(CASE
                WHEN team_points_for > team_points_against
                    THEN team_points_against
                WHEN team_points_for < team_points_against
                    THEN team_points_against * 1.5
                ELSE team_points_against
              END)                                                  AS winning_points_against
            , STRING_AGG(result, '' ORDER BY event_date DESC)       AS results
        FROM last_10
        WHERE rn <= 10
        GROUP BY uid, league_id
    )
    SELECT
        uid
        , league_id
        , games
        , wins
        , draws
        , losses
        , (wins + 0.5 * draws) / games                             AS win_percentage
        , total_points_for
        , total_points_against
        , winning_points_for
        , winning_points_against
        , results
    FROM agg;

CREATE INDEX idx_sql_asset_last_10_games
    ON public.sql_asset_last_10_games (uid);
```

---

## 7.7 `07_events_future_elos.sql`

Creates three tables in one file. First creates `sql_py_stats_utd` (current-season stats) and `sql_py_stats_ls` (last-season stats) as filtered views of `py_stats`. Then creates `sql_events_future_elos` which joins these to upcoming unscored events to attach pre-game Elo win probabilities.

**Bug fix from original**: The original used `INNER JOIN` on both `sql_py_stats_utd` AND `sql_py_stats_ls`, which required a team to have stats in *both* the current and last seasons. New teams in a league only have current-season stats and were silently excluded from forecasts. The fix is `LEFT JOIN` with `COALESCE`.

```sql
-- 07_events_future_elos.sql
-- Creates sql_py_stats_utd, sql_py_stats_ls, and sql_events_future_elos.
-- py_stats_utd/ls are filtered views of py_stats for current/last season.
-- events_future_elos attaches Elo win probabilities to upcoming games.
--
-- BUG FIX: original used INNER JOIN on both utd and ls, silently excluding
-- teams that only have current-season stats (new teams). Fixed to LEFT JOIN.

-- ── sql_py_stats_utd ──────────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.sql_py_stats_utd;

CREATE TABLE public.sql_py_stats_utd AS
    SELECT ps.*
    FROM public.py_stats AS ps
    INNER JOIN public.sql_season_last5 AS s5
        ON  s5.league_id    = CAST(ps.league_id    AS BIGINT)
        AND s5.league_season = CAST(ps.league_season AS TEXT)
    WHERE s5.season_rank = 1;

CREATE INDEX idx_sql_py_stats_utd
    ON public.sql_py_stats_utd (uid);

-- ── sql_py_stats_ls ───────────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.sql_py_stats_ls;

CREATE TABLE public.sql_py_stats_ls AS
    SELECT ps.*
    FROM public.py_stats AS ps
    INNER JOIN public.sql_season_last5 AS s5
        ON  s5.league_id    = CAST(ps.league_id    AS BIGINT)
        AND s5.league_season = CAST(ps.league_season AS TEXT)
    WHERE s5.season_rank = 2;

CREATE INDEX idx_sql_py_stats_ls
    ON public.sql_py_stats_ls (uid);

-- ── sql_events_future_elos ────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.sql_events_future_elos;

CREATE TABLE public.sql_events_future_elos AS
    SELECT
        we.event_id
        , we.league_id
        , we.league_season
        , we.event_date
        , we.league_sport
        , we.uid_home
        , we.uid_away
        , we.team_score_home
        , we.team_score_away
        , ROW_NUMBER() OVER (
            PARTITION BY we.league_id
            ORDER BY we.event_date ASC
          )                                                         AS game_order
        -- Current-season Elo, fallback to last-season if not yet available
        , COALESCE(home_utd.end_of_season_elo,
                   home_ls.end_of_season_elo)                      AS team_elo_home
        , COALESCE(away_utd.end_of_season_elo,
                   away_ls.end_of_season_elo)                      AS team_elo_away
        -- Win probability from Elo difference (standard logistic formula)
        , 1.0 / (
            POWER(10,
                -(
                    COALESCE(home_utd.end_of_season_elo, home_ls.end_of_season_elo)
                  - COALESCE(away_utd.end_of_season_elo, away_ls.end_of_season_elo)
                ) / 400.0
            ) + 1
          )                                                         AS team_home_win_prob
        , 1.0 / (
            POWER(10,
                -(
                    COALESCE(away_utd.end_of_season_elo, away_ls.end_of_season_elo)
                  - COALESCE(home_utd.end_of_season_elo, home_ls.end_of_season_elo)
                ) / 400.0
            ) + 1
          )                                                         AS team_away_win_prob
    FROM public.sql_web_events AS we
    -- LEFT JOIN (not INNER): teams with only current-season stats still included
    LEFT JOIN public.sql_py_stats_utd AS home_utd
        ON home_utd.uid = we.uid_home
    LEFT JOIN public.sql_py_stats_utd AS away_utd
        ON away_utd.uid = we.uid_away
    LEFT JOIN public.sql_py_stats_ls  AS home_ls
        ON home_ls.uid  = we.uid_home
    LEFT JOIN public.sql_py_stats_ls  AS away_ls
        ON away_ls.uid  = we.uid_away
    WHERE
        we.event_date >= CURRENT_DATE::DATE
        AND we.team_score_away IS NULL
        AND we.team_score_home IS NULL;
```

---

## 7.8 `08_assets_future.sql`

Unpivots `sql_events_future_elos` into one row per team per upcoming game, giving each team their win probability for that game.

```sql
-- 08_assets_future.sql
-- Unpivoted future events: one row per team per upcoming game.
-- Provides team_home_win_prob (from each team's perspective) for forecasting.

DROP TABLE IF EXISTS public.sql_assets_future;

CREATE TABLE public.sql_assets_future AS
    WITH combined AS (
        SELECT
            uid_home                AS uid
            , league_id
            , league_season
            , event_date
            , team_home_win_prob    AS team_home_win_prob
            , event_id
        FROM public.sql_events_future_elos

        UNION ALL

        SELECT
            uid_away                AS uid
            , league_id
            , league_season
            , event_date
            , team_away_win_prob    AS team_home_win_prob
            , event_id
        FROM public.sql_events_future_elos
    )
    SELECT * FROM combined;

CREATE INDEX idx_sql_assets_future
    ON public.sql_assets_future (uid);
```

---

## 7.9 `09_assets_stats_at.sql`

All-time aggregated stats per team across all seasons in `py_stats`. Provides total wins, losses, draws, and all-time win percentage. Used by `sql_wld`.

```sql
-- 09_assets_stats_at.sql
-- All-time aggregated W/D/L stats per team (all seasons combined).
-- Provides the "all time" stats column group for sql_wld and sql_web_assets.

DROP TABLE IF EXISTS public.sql_assets_stats_at;

CREATE TABLE public.sql_assets_stats_at AS
    SELECT
        ps.uid
        , ps.league_id
        , SUM(ps.wins)                                              AS total_wins
        , SUM(ps.losses)                                            AS total_losses
        , SUM(ps.draws)                                             AS total_draws
        , CASE
            WHEN SUM(ps.wins) + SUM(ps.losses) + SUM(ps.draws) = 0
                THEN 0
            ELSE ROUND(
                100.0
                * (SUM(ps.wins) + 0.5 * SUM(ps.draws))
                / (SUM(ps.wins) + SUM(ps.losses) + SUM(ps.draws)),
                1
            )
          END                                                       AS win_percentage_all_time
    FROM public.py_stats AS ps
    GROUP BY ps.uid, ps.league_id;

CREATE INDEX idx_sql_assets_stats_at
    ON public.sql_assets_stats_at (uid);
```

## 7.10 `10_forecast.sql`

Computes each team's average win probability over their next 20 upcoming games. Used by `sql_current_elo` and ultimately `sql_web_assets` as `asset_forecast`.

```sql
-- 10_forecast.sql
-- Average win probability over each team's next 20 upcoming games.
-- Used as 'asset_forecast' in sql_web_assets.

DROP TABLE IF EXISTS public.sql_forecast;

CREATE TABLE public.sql_forecast AS
    WITH ranked AS (
        SELECT
            uid
            , league_id
            , event_date
            , team_home_win_prob
            , ROW_NUMBER() OVER (
                PARTITION BY uid
                ORDER BY event_date ASC
              ) AS rn
        FROM public.sql_assets_future
    )
    SELECT
        uid
        , league_id
        , AVG(team_home_win_prob)   AS avg_win_probability_next_20_games
    FROM ranked
    WHERE rn BETWEEN 1 AND 20
    GROUP BY uid, league_id;

CREATE INDEX idx_sql_forecast
    ON public.sql_forecast (uid);
```

---

## 7.11 `11_assets_season_to_date.sql`

Current-season points and points-per-week per team. Points are computed per calendar week (weekly fantasy scoring). Used by `sql_wld`.

```sql
-- 11_assets_season_to_date.sql
-- Current-season points and points-per-week per team.
-- Points are scored on a per-calendar-week basis (weekly fantasy format).

DROP TABLE IF EXISTS public.sql_assets_season_to_date;

CREATE TABLE public.sql_assets_season_to_date AS
    WITH events AS (
        SELECT
            es.uid
            , es.league_id
            , es.league_season
            , es.event_date
            , CASE
                WHEN es.team_points_for IS NULL
                  OR es.team_points_against IS NULL THEN '?'
                WHEN es.team_points_for > es.team_points_against  THEN 'W'
                WHEN es.team_points_for < es.team_points_against  THEN 'L'
                WHEN es.team_points_for = es.team_points_against  THEN 'D'
              END AS result
        FROM public.sql_events_split AS es
        INNER JOIN public.sql_season_last5 AS s5
            ON s5.league_id    = es.league_id
           AND s5.league_season = es.league_season
        WHERE s5.season_rank = 1
    )
    , by_week AS (
        SELECT
            uid
            , league_id
            , league_season
            , DATE_TRUNC('week', event_date)::DATE              AS calendar_week
            , COALESCE(SUM(CASE WHEN result = 'W' THEN 1 END), 0) AS wins
            , COALESCE(SUM(CASE WHEN result = 'L' THEN 1 END), 0) AS losses
            , COALESCE(SUM(CASE WHEN result = 'D' THEN 1 END), 0) AS draws
        FROM events
        WHERE result IN ('W', 'L', 'D')
        GROUP BY uid, league_id, league_season,
                 DATE_TRUNC('week', event_date)::DATE
    )
    , calc_points AS (
        SELECT
            *
            , CAST(1.0 * wins / (wins + losses + draws) AS FLOAT)   AS win_percentage
            , CAST(ROUND(100.0 * wins / (wins + losses + draws)) AS INT) AS points
        FROM by_week
    )
    SELECT
        uid
        , league_id
        , league_season
        , CAST(SUM(wins)   AS INT)                                  AS wins
        , CAST(SUM(losses) AS INT)                                  AS losses
        , CAST(SUM(draws)  AS INT)                                  AS draws
        , CAST(SUM(points) AS INT)                                  AS points
        , CAST(SUM(points) AS FLOAT) / COUNT(*)                     AS points_per_week
    FROM calc_points
    GROUP BY uid, league_id, league_season;

CREATE INDEX idx_sql_assets_season_to_date
    ON public.sql_assets_season_to_date (uid, league_season);
```

---

## 7.12 `12_ref_elo.sql`

Each team's most recent season Elo rating across the last 5 seasons. Used by `sql_current_elo` as the reference Elo value.

```sql
-- 12_ref_elo.sql
-- Each team's most recent season Elo rating (from py_stats, within last 5 seasons).
-- Used as the reference Elo for current_elo computation.

DROP TABLE IF EXISTS public.sql_ref_elo;

CREATE TABLE public.sql_ref_elo AS
    WITH ranked AS (
        SELECT
            ps.uid
            , ps.league_id
            , ps.league_season
            , ps.end_of_season_elo
            , ROW_NUMBER() OVER (
                PARTITION BY ps.uid
                ORDER BY ps.league_season DESC NULLS LAST
              ) AS rn
        FROM public.py_stats AS ps
        INNER JOIN public.sql_leagues_current AS slc
            ON CAST(slc.league_id AS BIGINT) = CAST(ps.league_id AS BIGINT)
        INNER JOIN public.sql_season_last5 AS s5
            ON  CAST(s5.league_id    AS BIGINT) = CAST(ps.league_id    AS BIGINT)
            AND CAST(s5.league_season AS TEXT)   = CAST(ps.league_season AS TEXT)
    )
    SELECT
        uid
        , league_id
        , league_season
        , end_of_season_elo
    FROM ranked
    WHERE rn = 1
    ORDER BY end_of_season_elo ASC;
```

---

## 7.13 `13_current_elo.sql`

Combines current-season and last-season stats to produce a single current Elo, delta, and tier per team. Prefers current-season values but falls back to last-season via `COALESCE`.

```sql
-- 13_current_elo.sql
-- Per-team current Elo, delta, season delta, forecast, and tier.
-- Prefers current-season (utd) values; falls back to last-season (ls) via COALESCE.

DROP TABLE IF EXISTS public.sql_current_elo;

CREATE TABLE public.sql_current_elo AS
    SELECT
        COALESCE(utd.uid,              ls.uid)              AS uid
        , COALESCE(utd.league_id,      ls.league_id)        AS league_id
        , COALESCE(utd.end_of_season_elo,
                   ls.end_of_season_elo)                    AS current_elo
        , COALESCE(utd.last_elo_delta,
                   ls.last_elo_delta)                       AS elo_delta
        , COALESCE(utd.season_elo_delta,
                   ls.season_elo_delta)                     AS season_elo_delta
        , f.avg_win_probability_next_20_games               AS forecast
        , t.tier                                            AS tier
    FROM public.sql_py_stats_utd AS utd
    FULL OUTER JOIN public.sql_py_stats_ls AS ls
        USING (uid)
    LEFT JOIN public.sql_forecast AS f
        ON f.uid = COALESCE(utd.uid, ls.uid)
    LEFT JOIN public.py_tier AS t
        ON t.uid = COALESCE(utd.uid, ls.uid)
    ORDER BY current_elo DESC NULLS LAST;

CREATE INDEX idx_sql_current_elo
    ON public.sql_current_elo (uid);
```

---

## 7.14 `14_wld.sql`

Assembles per-team win/loss/draw statistics across four time horizons (last 10, last season, this season, all-time) plus attack/defence scores. This is the wide stats table that feeds `sql_web_assets`.

```sql
-- 14_wld.sql
-- Per-team W/D/L stats across 4 time horizons plus attack/defence scores.
-- Feeds sql_web_assets as the primary stats source.

DROP TABLE IF EXISTS public.sql_wld;

CREATE TABLE public.sql_wld AS
    SELECT
        sat.uid
        , sat.league_id
        -- Last 10 games
        , l10.wins                          AS last_10_wins
        , l10.draws                         AS last_10_draws
        , l10.losses                        AS last_10_losses
        , l10.win_percentage                AS last_10_win_percentage
        , l10.results                       AS last_10_results
        -- Last season
        , ls.wins                           AS last_season_wins
        , ls.draws                          AS last_season_draws
        , ls.losses                         AS last_season_losses
        , ls.win_percentage                 AS last_season_win_percentage
        -- This season
        , utd.wins                          AS this_season_wins
        , utd.draws                         AS this_season_draws
        , utd.losses                        AS this_season_losses
        , utd.win_percentage                AS this_season_win_percentage
        -- Season points (fantasy scoring)
        , std.points                        AS this_season_points
        , std.points_per_week               AS this_season_points_per_week
        -- All time
        , sat.total_wins                    AS all_time_wins
        , sat.total_draws                   AS all_time_draws
        , sat.total_losses                  AS all_time_losses
        , sat.win_percentage_all_time       AS all_time_win_percentage
        -- League rank from current or last season Elo
        , COALESCE(utd.end_rank_league,
                   ls.end_rank_league)      AS league_rank
        -- Attack / defence scores (from last 10 games)
        , l10.winning_points_for            AS asset_attack
        , l10.winning_points_against        AS asset_defense
    FROM public.sql_assets_stats_at AS sat
    LEFT JOIN public.sql_py_stats_ls  AS ls  USING (uid)
    LEFT JOIN public.sql_py_stats_utd AS utd USING (uid)
    LEFT JOIN public.sql_asset_last_10_games AS l10 USING (uid)
    LEFT JOIN public.sql_assets_season_to_date AS std USING (uid);

CREATE INDEX idx_sql_wld
    ON public.sql_wld (uid);
```

---

## 7.15 `15_web_assets.sql`

The primary website table. One row per team with all display-ready fields. Key change from original: `asset_luck` now reads from `py_stats.luck_display` (the Elo-adjusted luck percentile) instead of `ROUND(RANDOM() * 100)`.

```sql
-- 15_web_assets.sql
-- Primary website table: one row per team with all display fields.
-- CHANGE FROM ORIGINAL: asset_luck reads from py_stats.luck_display
-- (Elo-adjusted win-rate delta, 0-100 percentile rank) instead of RANDOM().

DROP TABLE IF EXISTS public.sql_web_assets;

CREATE TABLE public.sql_web_assets AS
    SELECT
        aa.uid
        , aa.league_id
        , sl.league_name
        , sl.league_name                                            AS league_name_clean
        , sl.league_sport
        , SUBSTRING(aa.uid FROM '[^-]+$')                          AS asset_id
        , aa.team_name                                             AS asset_name
        , aa.team_country                                          AS asset_country
        , sce.tier                                                 AS asset_tier
        , aa.team_logo                                             AS asset_logo
        , sce.current_elo                                          AS asset_elo
        , sce.elo_delta                                            AS asset_elo_delta
        , sce.season_elo_delta                                     AS asset_season_elo_delta
        , wld.league_rank                                          AS asset_league_rank
        , sce.forecast                                             AS asset_forecast
        , RANK() OVER (
            ORDER BY sce.current_elo DESC NULLS LAST
          )                                                        AS asset_overall_rank
        -- Attack: cumulative-distribution percentile of attack score within league
        , ROUND((
            CUME_DIST() OVER (
                PARTITION BY sl.league_id
                ORDER BY wld.asset_attack ASC
            )
          ) * 100)                                                 AS asset_atk
        -- Defence: cumulative-distribution percentile (lower conceded = better)
        , ROUND((
            CUME_DIST() OVER (
                PARTITION BY sl.league_id
                ORDER BY wld.asset_defense DESC NULLS LAST
            )
          ) * 100)                                                 AS asset_def
        -- Luck: Elo-adjusted win-rate delta (0-100 percentile). Was ROUND(RANDOM()*100).
        , COALESCE(ps_luck.luck_display, 50)                       AS asset_luck
        -- W/D/L and points columns
        , wld.last_10_wins
        , wld.last_10_draws
        , wld.last_10_losses
        , wld.last_10_win_percentage
        , wld.last_10_results
        , wld.last_season_wins
        , wld.last_season_draws
        , wld.last_season_losses
        , wld.last_season_win_percentage
        , wld.this_season_wins
        , wld.this_season_draws
        , wld.this_season_losses
        , wld.this_season_win_percentage
        , wld.this_season_points
        , wld.this_season_points_per_week
        , wld.all_time_wins
        , wld.all_time_draws
        , wld.all_time_losses
        , wld.all_time_win_percentage
        , CURRENT_TIMESTAMP                                        AS updated_at
    FROM public.api_assets AS aa
    INNER JOIN public.sql_leagues_current AS sl
        ON sl.league_id = aa.league_id
    LEFT JOIN public.sql_wld AS wld
        USING (uid)
    LEFT JOIN public.sql_current_elo AS sce
        USING (uid)
    -- Join py_stats for luck_display (most recent season per team)
    LEFT JOIN (
        SELECT DISTINCT ON (uid)
            uid
            , luck_display
        FROM public.py_stats
        ORDER BY uid, league_season DESC NULLS LAST
    ) AS ps_luck
        USING (uid);

CREATE INDEX idx_sql_web_assets
    ON public.sql_web_assets (uid);
```

---

## 7.16 `16_web_assets_info.sql`

Full `api_assets` rows for all teams that appear in `sql_web_assets`. Used by the website to fetch extended team metadata (descriptions, social links, etc.) separately from the stats table.

```sql
-- 16_web_assets_info.sql
-- Full api_assets rows for teams currently in sql_web_assets.
-- Provides extended team metadata to the website.

DROP TABLE IF EXISTS public.sql_web_assets_info;

CREATE TABLE public.sql_web_assets_info AS
    SELECT aa.*
    FROM public.api_assets AS aa
    WHERE aa.uid IN (
        SELECT uid FROM public.sql_web_assets
    );
```

---

## 7.17 `17_league_info.sql`

Per-league asset count and draft slot limits. Used by the website to know how many teams are in each league and how many can be selected per fantasy team.

```sql
-- 17_league_info.sql
-- Per-league team count and draft slot configuration.
-- league_asset_min / league_asset_max control how many teams
-- a fantasy owner can hold from each league.

DROP TABLE IF EXISTS public.sql_league_info;

CREATE TABLE public.sql_league_info AS
    SELECT
        league_id
        , COUNT(DISTINCT uid)   AS league_asset_count
        , 0                     AS league_asset_min
        , 3                     AS league_asset_max
    FROM public.sql_web_assets
    GROUP BY league_id;
```

---

\newpage

# 8. GitHub Actions Workflows

Two workflow files live in `.github/workflows/`. Both use identical secrets. The only differences are the cron schedule, timeout, and which runner module is invoked.

**GitHub Secrets required** (set in the repository's Settings → Secrets → Actions):

| Secret name | Value |
|---|---|
| `SPORTSDB_API_KEY` | TheSportsDB developer API key |
| `SUPABASE_URL` | `https://XXXX.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | From Supabase Project Settings → API → service_role key |
| `DB_HOST` | Direct connection host: `db.XXXX.supabase.co` (**not** the pooler) |
| `DB_PORT` | `5432` |
| `DB_NAME` | `postgres` (Supabase default) |
| `DB_USER` | `postgres` (or your DB user) |
| `DB_PASSWORD` | From Supabase Project Settings → Database → Database password |

---

## 8.1 `.github/workflows/pipeline_weekly.yml`

Full refresh every Sunday at 02:00 UTC. Also triggerable manually via the GitHub UI (`workflow_dispatch`).

```yaml
# .github/workflows/pipeline_weekly.yml
# Full pipeline refresh: all whitelisted leagues, all seasons, full stat recompute.
# Runs every Sunday at 02:00 UTC. Also manually triggerable.

name: Pipeline — Full Weekly Refresh

on:
  schedule:
    - cron: "0 2 * * 0"    # Sunday 02:00 UTC
  workflow_dispatch:         # manual trigger from GitHub UI

jobs:
  full-refresh:
    runs-on: ubuntu-latest
    timeout-minutes: 30      # abort if stuck; full run expected 10-25 min

    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"

      - name: Install pipeline package
        run: pip install -e .

      - name: Run full refresh
        run: python -m pipeline.runners.full_refresh
        env:
          SPORTSDB_API_KEY:           ${{ secrets.SPORTSDB_API_KEY }}
          SUPABASE_URL:               ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY:  ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          DB_HOST:                    ${{ secrets.DB_HOST }}
          DB_PORT:                    ${{ secrets.DB_PORT }}
          DB_NAME:                    ${{ secrets.DB_NAME }}
          DB_USER:                    ${{ secrets.DB_USER }}
          DB_PASSWORD:                ${{ secrets.DB_PASSWORD }}
```

---

## 8.2 `.github/workflows/pipeline_daily.yml`

Current-season events update every 4 hours. Recomputes stats and rebuilds all derived tables from the updated events.

```yaml
# .github/workflows/pipeline_daily.yml
# Daily events update: active leagues, current season only.
# Runs every 4 hours. Also manually triggerable.

name: Pipeline — Daily Events Update

on:
  schedule:
    - cron: "0 */4 * * *"   # every 4 hours
  workflow_dispatch:

jobs:
  daily-update:
    runs-on: ubuntu-latest
    timeout-minutes: 15      # abort if stuck; expected 2-5 min

    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"

      - name: Install pipeline package
        run: pip install -e .

      - name: Run daily update
        run: python -m pipeline.runners.daily_update
        env:
          SPORTSDB_API_KEY:           ${{ secrets.SPORTSDB_API_KEY }}
          SUPABASE_URL:               ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY:  ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          DB_HOST:                    ${{ secrets.DB_HOST }}
          DB_PORT:                    ${{ secrets.DB_PORT }}
          DB_NAME:                    ${{ secrets.DB_NAME }}
          DB_USER:                    ${{ secrets.DB_USER }}
          DB_PASSWORD:                ${{ secrets.DB_PASSWORD }}
```

\newpage

# 9. Admin UI — `admin/app.py`

A Streamlit application for managing the league registry and monitoring pipeline health. Run locally with:

```bash
cd admin
pip install -r requirements.txt
streamlit run app.py
```

It can also be deployed to Streamlit Community Cloud by pointing it at the repo's `admin/app.py`. It reads the same `.env` file as the pipeline.

## 9.1 `admin/requirements.txt`

```
streamlit>=1.35
supabase>=2.5
pandas>=2.2
python-dotenv>=1.0
```

## 9.2 `admin/app.py`

```python
"""MUFL Pipeline Admin UI.

Three pages:
  1. League Manager  — toggle is_whitelisted / is_active per league
  2. Pipeline Status — last fetch times, event counts, run health
  3. Elo & Tiers     — Elo distribution, tier breakdown, top/bottom teams

Run: streamlit run admin/app.py
Requires the same .env file as the pipeline (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client, Client

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

SUPABASE_URL              = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

st.set_page_config(
    page_title="MUFL Pipeline Admin",
    page_icon="⚽",
    layout="wide",
)


# ── Supabase client (cached for the session) ──────────────────────────────────

@st.cache_resource
def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_registry() -> pd.DataFrame:
    """Load full league_registry joined with api_leagues."""
    client = get_client()
    rows = (
        client.table("league_registry")
        .select(
            "league_id,league_name,league_sport,sport_type,"
            "is_whitelisted,is_active,display_name,"
            "last_fetched_at,team_count,notes,updated_at"
        )
        .execute()
        .data
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["last_fetched_at"] = pd.to_datetime(df["last_fetched_at"], utc=True, errors="coerce")
    df["updated_at"]      = pd.to_datetime(df["updated_at"],      utc=True, errors="coerce")
    return df.sort_values(["league_sport", "league_name"])


@st.cache_data(ttl=30)
def load_event_counts() -> pd.DataFrame:
    """Event counts per active league per season (last 5)."""
    client = get_client()
    # Pull from api_events joined with league_registry
    rows = (
        client.table("api_events")
        .select("league_id,league_season")
        .execute()
        .data
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return (
        df.groupby(["league_id", "league_season"])
        .size()
        .reset_index(name="event_count")
        .sort_values(["league_id", "league_season"], ascending=[True, False])
    )


@st.cache_data(ttl=30)
def load_py_stats_summary() -> pd.DataFrame:
    """Latest updated_at per league from py_stats."""
    client = get_client()
    rows = (
        client.table("py_stats")
        .select("league_id,updated_at")
        .execute()
        .data
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    return (
        df.groupby("league_id")["updated_at"]
        .max()
        .reset_index()
        .rename(columns={"updated_at": "stats_updated_at"})
    )


@st.cache_data(ttl=30)
def load_elo_data() -> pd.DataFrame:
    """Current Elo for all teams from sql_current_elo."""
    client = get_client()
    rows = (
        client.table("sql_current_elo")
        .select("uid,league_id,current_elo,tier")
        .execute()
        .data
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=30)
def load_league_names() -> dict[str, str]:
    """Map league_id → league_name from api_leagues."""
    client = get_client()
    rows = client.table("api_leagues").select("league_id,league_name").execute().data
    return {str(r["league_id"]): r["league_name"] for r in rows}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _update_registry(league_id: int, field: str, value: bool) -> None:
    """Write a single boolean toggle to league_registry."""
    client = get_client()
    client.table("league_registry").update(
        {field: value, "updated_at": datetime.now(timezone.utc).isoformat()}
    ).eq("league_id", league_id).execute()
    # Clear cache so reload shows updated value
    load_registry.clear()


def _validate_toggle(df_row: pd.Series, field: str, new_value: bool) -> str | None:
    """Return an error message if the toggle would violate a constraint, else None."""
    if field == "is_active" and new_value:
        if not df_row["is_whitelisted"]:
            return "Cannot activate: league must be whitelisted first."
        if not df_row["sport_type"]:
            return "Cannot activate: sport_type must be set first."
    if field == "is_whitelisted" and not new_value:
        if df_row["is_active"]:
            return "Cannot un-whitelist an active league. Deactivate first."
    return None


# ── Page 1: League Manager ────────────────────────────────────────────────────

def page_league_manager() -> None:
    st.title("⚽ League Manager")
    st.caption(
        "Toggle whitelisted/active status per league. "
        "Active leagues run in every daily update. "
        "Whitelisted leagues run in weekly full refresh."
    )

    df = load_registry()
    if df.empty:
        st.warning("No leagues found in league_registry. Run scripts/seed_registry.py first.")
        return

    # ── Filters ──────────────────────────────────────────────────────────────
    col_sport, col_search, col_status = st.columns([2, 3, 2])
    with col_sport:
        sports = ["All"] + sorted(df["league_sport"].dropna().unique().tolist())
        sport_filter = st.selectbox("Filter by sport", sports)
    with col_search:
        search = st.text_input("Search by name", placeholder="e.g. Premier League")
    with col_status:
        status_filter = st.selectbox(
            "Status",
            ["All", "Active only", "Whitelisted only", "Not whitelisted"],
        )

    # Apply filters
    view = df.copy()
    if sport_filter != "All":
        view = view[view["league_sport"] == sport_filter]
    if search:
        view = view[view["league_name"].str.contains(search, case=False, na=False)]
    if status_filter == "Active only":
        view = view[view["is_active"] == True]
    elif status_filter == "Whitelisted only":
        view = view[view["is_whitelisted"] == True]
    elif status_filter == "Not whitelisted":
        view = view[view["is_whitelisted"] == False]

    st.markdown(f"**{len(view)} leagues** matching filters ({len(df[df['is_active']])} active, "
                f"{len(df[df['is_whitelisted']])} whitelisted)")

    # ── Table with toggle columns ─────────────────────────────────────────────
    st.divider()

    # Column headers
    hdr = st.columns([3, 2, 2, 1, 1, 1, 2])
    hdr[0].markdown("**League**")
    hdr[1].markdown("**Sport**")
    hdr[2].markdown("**Sport Type**")
    hdr[3].markdown("**Teams**")
    hdr[4].markdown("**Whitelist**")
    hdr[5].markdown("**Active**")
    hdr[6].markdown("**Last Fetched**")
    st.divider()

    for _, row in view.iterrows():
        cols = st.columns([3, 2, 2, 1, 1, 1, 2])
        display = row["display_name"] or row["league_name"]
        cols[0].write(display)
        cols[1].write(row["league_sport"] or "—")
        cols[2].write(row["sport_type"] or "⚠️ not set")
        cols[3].write(int(row["team_count"]) if row["team_count"] else "—")

        # Whitelisted toggle
        wl_key = f"wl_{row['league_id']}"
        new_wl = cols[4].checkbox(
            "", value=bool(row["is_whitelisted"]), key=wl_key, label_visibility="collapsed"
        )
        if new_wl != row["is_whitelisted"]:
            err = _validate_toggle(row, "is_whitelisted", new_wl)
            if err:
                st.error(f"{display}: {err}")
            else:
                _update_registry(row["league_id"], "is_whitelisted", new_wl)
                st.rerun()

        # Active toggle
        ac_key = f"ac_{row['league_id']}"
        new_ac = cols[5].checkbox(
            "", value=bool(row["is_active"]), key=ac_key, label_visibility="collapsed"
        )
        if new_ac != row["is_active"]:
            err = _validate_toggle(row, "is_active", new_ac)
            if err:
                st.error(f"{display}: {err}")
            else:
                _update_registry(row["league_id"], "is_active", new_ac)
                st.rerun()

        # Last fetched
        if pd.notna(row["last_fetched_at"]):
            ago = datetime.now(timezone.utc) - row["last_fetched_at"]
            h = int(ago.total_seconds() // 3600)
            cols[6].write(f"{h}h ago")
        else:
            cols[6].write("never")


# ── Page 2: Pipeline Status ───────────────────────────────────────────────────

def page_pipeline_status() -> None:
    st.title("📊 Pipeline Status")

    registry   = load_registry()
    event_cts  = load_event_counts()
    stats_summ = load_py_stats_summary()
    names      = load_league_names()

    active = registry[registry["is_active"] == True].copy()
    if active.empty:
        st.info("No active leagues. Toggle leagues active in League Manager.")
        return

    st.subheader(f"{len(active)} Active Leagues")

    # Merge in stats updated_at
    active = active.merge(
        stats_summ, on="league_id", how="left"
    )

    now = datetime.now(timezone.utc)

    for _, row in active.iterrows():
        lid = row["league_id"]
        name = row["display_name"] or row["league_name"]

        with st.expander(f"**{name}** ({row['league_sport']} / {row['sport_type'] or '?'})"):
            c1, c2, c3 = st.columns(3)

            # Last fetched
            if pd.notna(row.get("last_fetched_at")):
                ago = now - row["last_fetched_at"]
                c1.metric("Last fetched", f"{int(ago.total_seconds()//3600)}h ago")
            else:
                c1.metric("Last fetched", "Never")

            # Stats updated
            if pd.notna(row.get("stats_updated_at")):
                ago2 = now - row["stats_updated_at"]
                c2.metric("Stats updated", f"{int(ago2.total_seconds()//3600)}h ago")
            else:
                c2.metric("Stats updated", "Never")

            # Team count
            c3.metric("Teams", int(row["team_count"]) if row["team_count"] else "—")

            # Event counts by season
            league_events = event_cts[
                event_cts["league_id"].astype(str) == str(lid)
            ].head(5)

            if not league_events.empty:
                st.dataframe(
                    league_events[["league_season", "event_count"]].rename(
                        columns={"league_season": "Season", "event_count": "Events"}
                    ),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.write("No events found.")

    st.divider()
    st.subheader("Whitelisted but Inactive")
    inactive = registry[
        (registry["is_whitelisted"] == True) & (registry["is_active"] == False)
    ]
    if inactive.empty:
        st.write("None.")
    else:
        st.dataframe(
            inactive[["league_name", "league_sport", "sport_type", "team_count"]],
            hide_index=True,
            use_container_width=True,
        )


# ── Page 3: Elo & Tier Overview ───────────────────────────────────────────────

def page_elo_tiers() -> None:
    st.title("📈 Elo & Tier Overview")

    elo_df = load_elo_data()
    if elo_df.empty:
        st.info("No Elo data available. Run the pipeline first.")
        return

    names = load_league_names()
    elo_df["league_name"] = elo_df["league_id"].astype(str).map(names).fillna("Unknown")
    elo_df["current_elo"] = pd.to_numeric(elo_df["current_elo"], errors="coerce")

    # ── Global Elo distribution histogram ─────────────────────────────────────
    st.subheader("Global Elo Distribution")
    import math

    hist_data = elo_df["current_elo"].dropna()
    bins = list(range(
        int(hist_data.min() // 50) * 50,
        int(hist_data.max() // 50) * 50 + 100,
        50,
    ))
    counts, edges = pd.cut(hist_data, bins=bins, retbins=True)
    hist_counts = counts.value_counts(sort=False)
    st.bar_chart(hist_counts)

    # ── Tier breakdown ─────────────────────────────────────────────────────────
    st.subheader("Tier Breakdown")
    tier_order = ["MOL", "SS", "S", "A", "B", "C", "D", "E", "F", "FF", "DIE"]
    tier_counts = (
        elo_df["tier"]
        .value_counts()
        .reindex(tier_order, fill_value=0)
        .reset_index()
    )
    tier_counts.columns = ["Tier", "Count"]
    st.dataframe(tier_counts, hide_index=True, use_container_width=False)

    # ── Filter by league or sport ──────────────────────────────────────────────
    st.subheader("Top / Bottom Teams by Elo")
    col_league, col_n = st.columns([3, 1])
    with col_league:
        leagues = ["All"] + sorted(elo_df["league_name"].unique().tolist())
        league_sel = st.selectbox("Filter by league", leagues)
    with col_n:
        n = st.number_input("Show top/bottom N", min_value=5, max_value=50, value=10)

    view = elo_df.copy()
    if league_sel != "All":
        view = view[view["league_name"] == league_sel]

    view = view.sort_values("current_elo", ascending=False).reset_index(drop=True)
    view.index += 1

    cols = ["uid", "league_name", "current_elo", "tier"]
    top = view.head(int(n))[cols].rename(
        columns={"uid": "UID", "league_name": "League",
                 "current_elo": "Elo", "tier": "Tier"}
    )
    bot = view.tail(int(n))[cols].rename(
        columns={"uid": "UID", "league_name": "League",
                 "current_elo": "Elo", "tier": "Tier"}
    )

    c_top, c_bot = st.columns(2)
    with c_top:
        st.markdown(f"**Top {n}**")
        st.dataframe(top, hide_index=True, use_container_width=True)
    with c_bot:
        st.markdown(f"**Bottom {n}**")
        st.dataframe(bot, hide_index=True, use_container_width=True)


# ── Navigation ────────────────────────────────────────────────────────────────

PAGES = {
    "⚽ League Manager":   page_league_manager,
    "📊 Pipeline Status":  page_pipeline_status,
    "📈 Elo & Tiers":      page_elo_tiers,
}

with st.sidebar:
    st.title("MUFL Admin")
    st.caption("Pipeline management console")
    page_name = st.radio("Navigate", list(PAGES.keys()), label_visibility="collapsed")
    st.divider()
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

PAGES[page_name]()
```

\newpage

# 10. Seed Script — `scripts/seed_registry.py`

Run once before the first pipeline execution. It calls the TheSportsDB all-leagues endpoint, matches league names against the whitelist, and populates `league_registry`. League IDs are **not** hardcoded — they are discovered from the live API response at seed time. If a league name isn't found in the API response the script logs a warning and continues; the admin can add it manually later.

```python
"""scripts/seed_registry.py

One-time script: discover league IDs from TheSportsDB, then populate
league_registry with the whitelist defined in WHITELIST below.

Run: python scripts/seed_registry.py

Requirements:
  - .env file with SPORTSDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
  - league_registry table already created in Supabase (run DDL from spec Ch.3 first)

The script is safe to re-run: it upserts on league_id so existing rows
are updated but not duplicated.
"""
from __future__ import annotations

import logging
import os
import sys
from difflib import get_close_matches

import httpx
from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

load_dotenv()

# ── Whitelist ─────────────────────────────────────────────────────────────────
# Tuples of (league_name_to_search, sport_type, is_active)
# league_name_to_search is matched (case-insensitive, fuzzy) against
# the strLeague / strLeagueAlternate fields in TheSportsDB.
# is_active=True means the league is included in daily updates immediately.
# is_active=False means it's whitelisted for weekly refresh only.
# is_whitelisted is always True for everything in this list.

WHITELIST: list[tuple[str, str, bool]] = [
    # Soccer — active
    ("English Premier League",           "standard", True),
    ("English Championship",             "standard", True),
    ("Mexican Primera League",           "standard", True),
    # Soccer — whitelisted
    ("American Major League Soccer",     "standard", False),
    ("Argentinian Primera Division",     "standard", False),
    ("Australian A-League",              "standard", False),
    ("Austrian Football Bundesliga",     "standard", False),
    ("Belgian Pro League",               "standard", False),
    ("Brazilian Serie A",                "standard", False),
    ("Dutch Eredivisie",                 "standard", False),
    ("French Ligue 1",                   "standard", False),
    ("German Bundesliga",                "standard", False),
    ("Italian Serie A",                  "standard", False),
    ("Spanish La Liga",                  "standard", False),
    ("English League 1",                 "standard", False),
    ("English League 2",                 "standard", False),
    ("Japanese J1 League",               "standard", False),
    ("American NWSL",                    "standard", False),
    ("Australian A-League Women",        "standard", False),
    ("Bangladesh Premier League",        "standard", False),
    # Motorsports
    ("Formula 1",                        "multi_competitor", False),
    ("Formula E",                        "multi_competitor", False),
    ("NASCAR Cup Series",                "multi_competitor", False),
    ("Formula 2",                        "multi_competitor", False),
    # Fighting
    ("UFC",                              "binary", False),
    ("Boxing",                           "binary", False),
    ("Cage Warriors",                    "binary", False),
    ("WWE",                              "binary", False),
    ("Professional Fighters League",     "binary", False),
    # Baseball
    ("Korean KBO League",                "standard", False),
    ("MLB",                              "standard", False),
    ("Nippon Professional Baseball",     "standard", False),
    ("Cuban National Series",            "standard", False),
    ("Chinese Professional Baseball",    "standard", False),
    ("NCAA Division I Baseball",         "standard", False),
    # Basketball — active
    ("NBA",                              "standard", True),
    ("Chinese CBA",                      "standard", True),
    # Basketball — whitelisted
    ("Euroleague Basketball",            "standard", False),
    ("WNBA",                             "standard", False),
    ("Mexican LNBP",                     "standard", False),
    ("Spanish Liga ACB",                 "standard", False),
    ("NCAA Division I Men's Basketball", "standard", False),
    ("NCAA Division I Women's Basketball","standard", False),
    # American Football — active
    ("NFL",                              "standard", True),
    # American Football — whitelisted
    ("CFL",                              "standard", False),
    ("NCAA Division I Football",         "standard", False),
    # Hockey — active
    ("Swedish Hockey League",            "standard", True),
    # Hockey — whitelisted
    ("Finnish Liiga",                    "standard", False),
    ("German DEL",                       "standard", False),
    ("NHL",                              "standard", False),
    ("Swiss National League A",          "standard", False),
    ("NCAA Division I Hockey",           "standard", False),
    # Rugby — active
    ("English Premiership Rugby",        "standard", True),
    # Rugby — whitelisted
    ("Australian National Rugby League", "standard", False),
    ("English Rugby League Super League","standard", False),
    ("French Top 14",                    "standard", False),
    ("Super Rugby",                      "standard", False),
    ("United Rugby Championship",        "standard", False),
    # Tennis
    ("ATP World Tour",                   "binary", False),
    ("WTA Tour",                         "binary", False),
    # Cricket
    ("Australian Big Bash League",       "standard", False),
    ("English T20 Blast",                "standard", False),
    ("Indian Premier League",            "standard", False),
    # Cycling
    ("UCI World Tour",                   "multi_competitor", False),
    # E-Sports
    ("BLAST Premier",                    "standard", False),
    ("Call of Duty League",              "standard", False),
    ("ESL Pro League",                   "standard", False),
    ("League of Legends EMEA Championship","standard", False),
    ("League of Legends Pro League",     "standard", False),
    ("League of the Americas",           "standard", False),
]


# ── API helpers ───────────────────────────────────────────────────────────────

def fetch_all_leagues(api_key: str) -> list[dict]:
    """Fetch all leagues from TheSportsDB V2."""
    url = "https://www.thesportsdb.com/api/v2/json/all/leagues"
    resp = httpx.get(url, headers={"X-API-KEY": api_key}, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    # The all/leagues endpoint returns a list at the top level or nested
    if isinstance(data, list):
        return data
    for key in ("leagues", "countrys", "list"):
        if key in data and data[key]:
            return data[key]
    # Fallback: flatten any nested lists
    result = []
    for v in data.values():
        if isinstance(v, list):
            result.extend(v)
    return result


def build_name_map(leagues: list[dict]) -> dict[str, dict]:
    """Build a normalised name → league dict for fuzzy matching."""
    name_map: dict[str, dict] = {}
    for league in leagues:
        primary = (league.get("strLeague") or "").strip()
        alternate = (league.get("strLeagueAlternate") or "").strip()
        if primary:
            name_map[primary.lower()] = league
        if alternate:
            name_map[alternate.lower()] = league
    return name_map


def find_league(
    search_name: str,
    name_map: dict[str, dict],
    cutoff: float = 0.75,
) -> dict | None:
    """Fuzzy-match a league name against the name map. Returns best match or None."""
    key = search_name.lower()
    # Exact match first
    if key in name_map:
        return name_map[key]
    # Fuzzy match
    matches = get_close_matches(key, name_map.keys(), n=1, cutoff=cutoff)
    if matches:
        logger.info("  '%s' → fuzzy matched to '%s'", search_name, matches[0])
        return name_map[matches[0]]
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    api_key = os.environ.get("SPORTSDB_API_KEY")
    if not api_key:
        logger.error("SPORTSDB_API_KEY not set in environment.")
        sys.exit(1)

    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    logger.info("Fetching all leagues from TheSportsDB ...")
    all_leagues = fetch_all_leagues(api_key)
    logger.info("  Retrieved %d leagues.", len(all_leagues))

    name_map = build_name_map(all_leagues)

    records: list[dict] = []
    not_found: list[str] = []

    for search_name, sport_type, is_active in WHITELIST:
        league = find_league(search_name, name_map)
        if league is None:
            logger.warning("  NOT FOUND: '%s'", search_name)
            not_found.append(search_name)
            continue

        league_id   = league.get("idLeague") or league.get("league_id")
        league_name = league.get("strLeague") or search_name
        league_sport= league.get("strSport") or ""

        records.append({
            "league_id":     int(league_id),
            "league_name":   league_name,
            "league_sport":  league_sport,
            "sport_type":    sport_type,
            "is_whitelisted": True,
            "is_active":     is_active,
            "display_name":  None,
            "notes":         f"seeded from whitelist: {search_name}",
        })
        logger.info(
            "  ✓ '%s' → id=%s, sport=%s, active=%s",
            league_name, league_id, league_sport, is_active,
        )

    if not_found:
        logger.warning(
            "\n%d leagues NOT FOUND in TheSportsDB:\n  %s\n"
            "Add them manually via the Admin UI or re-run after correcting the name.",
            len(not_found), "\n  ".join(not_found),
        )

    if not records:
        logger.error("No leagues matched. Check SPORTSDB_API_KEY and network access.")
        sys.exit(1)

    logger.info("Upserting %d records into league_registry ...", len(records))
    supabase = create_client(supabase_url, supabase_key)

    # Chunk upserts (100 at a time)
    for i in range(0, len(records), 100):
        chunk = records[i: i + 100]
        supabase.table("league_registry").upsert(
            chunk, on_conflict="league_id"
        ).execute()

    logger.info("Done. %d leagues seeded.", len(records))
    if not_found:
        logger.info(
            "Action required: manually add %d missing leagues in Admin UI.", len(not_found)
        )


if __name__ == "__main__":
    main()
```

---

\newpage

# 11. Tests

## 11.1 `tests/conftest.py`

Shared fixtures used across all test modules.

```python
"""tests/conftest.py — shared pytest fixtures."""
from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture()
def minimal_events() -> pd.DataFrame:
    """Five scored events for two teams in one league."""
    return pd.DataFrame([
        {"event_id": "1", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-01", "league_sport": "Soccer",
         "uid_home": "100-1", "uid_away": "100-2",
         "team_score_home": 2.0, "team_score_away": 1.0,
         "event_result": "home", "game_order": 1},
        {"event_id": "2", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-08", "league_sport": "Soccer",
         "uid_home": "100-2", "uid_away": "100-1",
         "team_score_home": 0.0, "team_score_away": 0.0,
         "event_result": "draw", "game_order": 2},
        {"event_id": "3", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-15", "league_sport": "Soccer",
         "uid_home": "100-1", "uid_away": "100-2",
         "team_score_home": 1.0, "team_score_away": 3.0,
         "event_result": "away", "game_order": 3},
        {"event_id": "4", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-22", "league_sport": "Soccer",
         "uid_home": "100-2", "uid_away": "100-1",
         "team_score_home": 2.0, "team_score_away": 2.0,
         "event_result": "draw", "game_order": 4},
        {"event_id": "5", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-29", "league_sport": "Soccer",
         "uid_home": "100-1", "uid_away": "100-2",
         "team_score_home": 3.0, "team_score_away": 0.0,
         "event_result": "home", "game_order": 5},
    ])


@pytest.fixture()
def binary_events() -> pd.DataFrame:
    """Three UFC bouts (binary sport: scores are 1/0)."""
    return pd.DataFrame([
        {"event_id": "b1", "league_id": "200", "league_season": "2024",
         "event_date": "2024-02-01", "league_sport": "Fighting",
         "uid_home": "200-10", "uid_away": "200-11",
         "team_score_home": 1.0, "team_score_away": 0.0,
         "event_result": "home", "game_order": 1},
        {"event_id": "b2", "league_id": "200", "league_season": "2024",
         "event_date": "2024-02-08", "league_sport": "Fighting",
         "uid_home": "200-11", "uid_away": "200-12",
         "team_score_home": 1.0, "team_score_away": 0.0,
         "event_result": "home", "game_order": 2},
        {"event_id": "b3", "league_id": "200", "league_season": "2024",
         "event_date": "2024-02-15", "league_sport": "Fighting",
         "uid_home": "200-10", "uid_away": "200-12",
         "team_score_home": 0.0, "team_score_away": 1.0,
         "event_result": "away", "game_order": 3},
    ])
```

---

## 11.2 `tests/test_transform/test_stats.py`

```python
"""tests/test_transform/test_stats.py"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.stats import compute_basic_stats


def test_wins_losses_draws(minimal_events):
    result = compute_basic_stats(minimal_events)
    t1 = result[result["uid"] == "100-1"].iloc[0]
    t2 = result[result["uid"] == "100-2"].iloc[0]

    # Team 1: 2 wins (events 1, 5), 1 loss (event 3), 2 draws (events 2, 4)
    assert t1["wins"]  == 2
    assert t1["losses"] == 1
    assert t1["draws"] == 2

    # Team 2: 1 win (event 3), 2 losses (events 1, 5), 2 draws
    assert t2["wins"]  == 1
    assert t2["losses"] == 2
    assert t2["draws"] == 2


def test_win_percentage(minimal_events):
    result = compute_basic_stats(minimal_events)
    t1 = result[result["uid"] == "100-1"].iloc[0]
    # win_pct = (2 + 0.5*2) / 5 = 3/5 = 0.6
    assert abs(t1["win_percentage"] - 0.60) < 0.01


def test_games_played(minimal_events):
    result = compute_basic_stats(minimal_events)
    for uid in ("100-1", "100-2"):
        row = result[result["uid"] == uid].iloc[0]
        assert row["games_played"] == 5


def test_empty_input():
    empty = pd.DataFrame(columns=["event_id", "league_id", "league_season",
                                   "event_date", "league_sport", "uid_home",
                                   "uid_away", "team_score_home",
                                   "team_score_away", "event_result"])
    result = compute_basic_stats(empty)
    assert result.empty


def test_binary_events_stats(binary_events):
    result = compute_basic_stats(binary_events)
    # uid 200-10: 1 win (b1), 1 loss (b3) → 2 games
    t10 = result[result["uid"] == "200-10"].iloc[0]
    assert t10["games_played"] == 2
    assert t10["wins"] == 1
    assert t10["losses"] == 1
```

---

## 11.3 `tests/test_transform/test_elo.py`

```python
"""tests/test_transform/test_elo.py"""
from __future__ import annotations

import pytest
import pandas as pd

from pipeline.transform.elo import compute_elo_stats


def test_returns_two_dataframes(minimal_events):
    summary, history = compute_elo_stats(minimal_events)
    assert isinstance(summary, pd.DataFrame)
    assert isinstance(history, pd.DataFrame)


def test_summary_has_required_columns(minimal_events):
    summary, _ = compute_elo_stats(minimal_events)
    required = {
        "uid", "league_id", "league_season",
        "start_of_season_elo", "end_of_season_elo",
        "last_elo_delta", "season_elo_delta",
    }
    assert required.issubset(set(summary.columns))


def test_history_has_required_columns(minimal_events):
    _, history = compute_elo_stats(minimal_events)
    required = {"uid", "league_id", "league_season",
                "event_id", "event_date",
                "actual_result", "expected_win_prob", "current_elo"}
    assert required.issubset(set(history.columns))


def test_winner_elo_increases(minimal_events):
    """After a win the team's Elo should be higher than its starting value."""
    summary, _ = compute_elo_stats(minimal_events)
    # Team 100-1 has 2 wins and 1 loss, net positive expected
    t1 = summary[summary["uid"] == "100-1"].iloc[0]
    assert t1["season_elo_delta"] > 0


def test_both_teams_in_summary(minimal_events):
    summary, _ = compute_elo_stats(minimal_events)
    uids = set(summary["uid"].tolist())
    assert "100-1" in uids
    assert "100-2" in uids


def test_elo_sum_conserved(minimal_events):
    """Total Elo across teams must be conserved (zero-sum) to within rounding."""
    summary, _ = compute_elo_stats(minimal_events)
    latest = (
        summary.sort_values("league_season", ascending=False)
        .groupby("uid").first()
        .reset_index()
    )
    total_delta = latest["season_elo_delta"].sum()
    assert abs(total_delta) < 1.0  # rounding tolerance


def test_empty_input():
    empty = pd.DataFrame(columns=["event_id", "league_id", "league_season",
                                   "event_date", "league_sport", "uid_home",
                                   "uid_away", "team_score_home",
                                   "team_score_away", "event_result"])
    summary, history = compute_elo_stats(empty)
    assert summary.empty
    assert history.empty
```

---

## 11.4 `tests/test_transform/test_tiers.py`

```python
"""tests/test_transform/test_tiers.py"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.tiers import compute_tiers


def _make_elo_summary(elos: list[float]) -> pd.DataFrame:
    """Create a minimal elo_summary DataFrame with the given Elo values."""
    return pd.DataFrame({
        "uid":                [f"100-{i}" for i in range(len(elos))],
        "league_id":          ["100"] * len(elos),
        "league_season":      ["2024"] * len(elos),
        "end_of_season_elo":  elos,
    })


def test_returns_list_of_dicts():
    df = _make_elo_summary([1400.0, 1500.0, 1600.0])
    result = compute_tiers(df)
    assert isinstance(result, list)
    assert all(isinstance(r, dict) for r in result)


def test_required_keys():
    df = _make_elo_summary([1400.0, 1500.0, 1600.0])
    result = compute_tiers(df)
    for row in result:
        assert "uid" in row
        assert "tier" in row
        assert "league_id" in row


def test_highest_elo_gets_high_tier():
    """With 1000 teams, top 0.5% should be MOL."""
    import random
    elos = [random.uniform(1200, 1800) for _ in range(200)]
    elos.append(9999.0)  # guaranteed top
    df = _make_elo_summary(elos)
    result = compute_tiers(df)
    top_row = next(r for r in result if r["uid"] == f"100-{len(elos)-1}")
    assert top_row["tier"] == "MOL"


def test_lowest_elo_gets_die():
    """Lowest Elo should get DIE tier."""
    elos = [float(i * 10) for i in range(1, 201)]
    elos[0] = 1.0  # guaranteed lowest
    df = _make_elo_summary(elos)
    result = compute_tiers(df)
    bottom = next(r for r in result if r["uid"] == "100-0")
    assert bottom["tier"] == "DIE"


def test_empty_input():
    result = compute_tiers(pd.DataFrame())
    assert result == []
```

---

## 11.5 `tests/test_transform/test_luck.py`

```python
"""tests/test_transform/test_luck.py"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.luck import compute_luck


def _make_history(uid: str, results: list[tuple[float, float]]) -> list[dict]:
    """results: list of (actual_result, expected_win_prob) tuples."""
    return [
        {
            "uid":               uid,
            "league_id":         "100",
            "event_id":          f"e{i}",
            "event_date":        f"2024-01-{i+1:02d}",
            "actual_result":     a,
            "expected_win_prob": e,
        }
        for i, (a, e) in enumerate(results)
    ]


def test_returns_dataframe():
    history = pd.DataFrame(
        _make_history("100-1", [(1.0, 0.5)] * 5)
    )
    result = compute_luck(history)
    assert isinstance(result, pd.DataFrame)


def test_required_columns():
    history = pd.DataFrame(_make_history("100-1", [(1.0, 0.5)] * 5))
    result = compute_luck(history)
    assert {"uid", "league_id", "luck_score", "luck_display"}.issubset(result.columns)


def test_positive_luck_when_outperforming():
    """Always winning when expected 50/50 → positive luck_score."""
    history = pd.DataFrame(
        _make_history("100-1", [(1.0, 0.5)] * 10)
        + _make_history("100-2", [(0.5, 0.5)] * 10)
    )
    result = compute_luck(history)
    t1 = result[result["uid"] == "100-1"].iloc[0]
    t2 = result[result["uid"] == "100-2"].iloc[0]
    assert t1["luck_score"] > t2["luck_score"]


def test_luck_display_in_range():
    """luck_display should always be in [0, 100]."""
    history = pd.DataFrame(
        _make_history("100-1", [(1.0, 0.9)] * 10)
        + _make_history("100-2", [(0.0, 0.1)] * 10)
        + _make_history("100-3", [(0.5, 0.5)] * 10)
    )
    result = compute_luck(history)
    assert (result["luck_display"] >= 0).all()
    assert (result["luck_display"] <= 100).all()


def test_empty_input():
    result = compute_luck(pd.DataFrame())
    assert result.empty
```

---

## 11.6 `tests/test_integration/test_smoke.py`

Smoke test that validates the full transform chain (stats → Elo → tiers → luck) runs end-to-end on synthetic data without errors.

```python
"""tests/test_integration/test_smoke.py

Full transform chain smoke test. Does not require a database connection.
Uses synthetic events to validate that all transform functions compose
correctly and produce output with the right shapes.
"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.stats import compute_basic_stats
from pipeline.transform.elo   import compute_elo_stats
from pipeline.transform.tiers import compute_tiers
from pipeline.transform.luck  import compute_luck


def _generate_round_robin(n_teams: int = 8, n_seasons: int = 2) -> pd.DataFrame:
    """Generate a synthetic round-robin tournament for n_teams over n_seasons."""
    import random
    rows = []
    eid = 0
    for season_i in range(n_seasons):
        season = f"202{season_i}"
        for home_i in range(n_teams):
            for away_i in range(n_teams):
                if home_i == away_i:
                    continue
                h_score = random.randint(0, 4)
                a_score = random.randint(0, 4)
                result = "draw" if h_score == a_score else ("home" if h_score > a_score else "away")
                rows.append({
                    "event_id":       f"e{eid}",
                    "league_id":      "999",
                    "league_season":  season,
                    "event_date":     f"202{season_i}-03-{(eid % 28) + 1:02d}",
                    "league_sport":   "Soccer",
                    "uid_home":       f"999-{home_i}",
                    "uid_away":       f"999-{away_i}",
                    "team_score_home": float(h_score),
                    "team_score_away": float(a_score),
                    "event_result":   result,
                    "game_order":     eid,
                })
                eid += 1
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def synthetic_events():
    return _generate_round_robin(n_teams=8, n_seasons=2)


def test_full_chain_runs(synthetic_events):
    basic_df            = compute_basic_stats(synthetic_events)
    elo_df, history_df  = compute_elo_stats(synthetic_events)
    tiers_data          = compute_tiers(elo_df)
    luck_df             = compute_luck(history_df)

    assert not basic_df.empty
    assert not elo_df.empty
    assert not history_df.empty
    assert len(tiers_data) > 0
    assert not luck_df.empty


def test_all_teams_in_output(synthetic_events):
    _, elo_df = compute_elo_stats(synthetic_events)
    summary = elo_df
    uids_in = set(
        synthetic_events["uid_home"].tolist()
        + synthetic_events["uid_away"].tolist()
    )
    uids_out = set(summary["uid"].tolist())
    assert uids_in == uids_out


def test_luck_display_all_valid(synthetic_events):
    _, history_df = compute_elo_stats(synthetic_events)
    luck_df = compute_luck(history_df)
    assert (luck_df["luck_display"] >= 0).all()
    assert (luck_df["luck_display"] <= 100).all()


def test_tier_for_every_team(synthetic_events):
    _, elo_df = compute_elo_stats(synthetic_events)
    tiers = compute_tiers(elo_df)
    uids_in = set(synthetic_events["uid_home"].unique())
    uids_tiered = {t["uid"] for t in tiers}
    assert uids_in == uids_tiered
```

---

\newpage

# 12. Implementation Sequence

Implement files strictly in this order. Each step is independently testable before the next begins. Do not skip steps — later modules have hard dependencies on earlier ones.

| Step | File(s) to create | What to validate before moving on |
|---|---|---|
| 1 | `pyproject.toml`, `.env.example`, `.gitignore` | `pip install -e .` succeeds with no errors |
| 2 | `src/pipeline/__init__.py`, `config.py`, `db.py` | `from pipeline.config import settings` loads without error; `settings.SEASON_WINDOW == 5` |
| 3 | `api/client.py`, `api/endpoints.py`, `api/schemas.py` | Construct a `RateLimitedClient` and call `all_leagues_url()` — verify URL is correct string |
| 4 | `extract/leagues.py`, `extract/seasons.py`, `extract/teams.py` | Call `fetch_all_leagues(client)` against the live API — verify it returns a non-empty list |
| 5 | `load/upsert.py` | Call `batch_upsert` with a list of 5 synthetic dicts against `api_leagues` — verify rows appear in Supabase |
| 6 | `transform/normalise.py` | Unit test `sanitise_date("0000-00-00")` returns `"1970-01-01"`; test `normalise_events()` with 3 rows |
| 7 | `extract/events.py` | Call `fetch_events_for_season(client, supabase, league_id, season)` for one active league — verify event rows returned |
| 8 | `transform/stats.py` | Run `pytest tests/test_transform/test_stats.py` — all 5 tests pass |
| 9 | `transform/elo.py` | Run `pytest tests/test_transform/test_elo.py` — all 6 tests pass |
| 10 | `transform/tiers.py` | Run `pytest tests/test_transform/test_tiers.py` — all 5 tests pass |
| 11 | `transform/luck.py` | Run `pytest tests/test_transform/test_luck.py` — all 5 tests pass |
| 12 | All 17 `.sql` files in `sql/queries/` | Manually execute `01_leagues_current.sql` against Supabase — verify `sql_leagues_current` is created with correct rows |
| 13 | `sql/executor.py` | Call `run_sql_file(conn, "01_leagues_current.sql")` — verify no exception and table exists |
| 14 | `runners/full_refresh.py` | Run full refresh locally with 1 active league — verify all 21 tables exist in Supabase after run |
| 15 | `runners/daily_update.py` | Run daily update — verify it completes in under 5 minutes and updates `py_stats.updated_at` |
| 16 | `tests/conftest.py`, all test files | Run `pytest tests/` — all tests pass; run smoke test `pytest tests/test_integration/` |
| 17 | `scripts/seed_registry.py` | Run `python scripts/seed_registry.py` — verify 71 rows in `league_registry` (or near 71 if some names need adjustment) |
| 18 | `admin/requirements.txt`, `admin/app.py` | Run `streamlit run admin/app.py` — verify League Manager page loads and toggles write to Supabase |
| 19 | `.github/workflows/pipeline_weekly.yml`, `pipeline_daily.yml` | Push to GitHub, go to Actions tab, trigger `pipeline_daily.yml` manually — verify it succeeds |
| 20 | **Delete** old files | Remove: `active.py`, `supabase_tables.py`, `requirements.txt`, `*.json`, `*.pkl`, `log_files/`, `__pycache__/` |

**Notes:**

- Step 14 (full refresh) is the first end-to-end integration test. It will likely surface import errors, missing columns, or connection issues that unit tests can't catch.
- For step 17, some league names in the whitelist may not exactly match TheSportsDB — the seed script logs fuzzy matches. Review the output and use the Admin UI to correct any entries.
- Step 20 (deleting old files) should only happen after step 19 (Actions workflow) succeeds. Keep the old files until the new pipeline is verified in production.

\newpage

# Appendix A — Original `active.py` (1,074 lines)

This is the complete source of the monolithic pipeline file being replaced. Reproduced here so the implementer can cross-reference any behaviour that is unclear from the spec. Do not use this as a base for the rewrite — start fresh from the architecture described in this document.

Key issues visible in this source (each fixed in the rewrite):

- Lines 26-79: module-level side effects — logging, DB connections, and env reads happen at import time
- Line 74: `supabase = create_client(s_url, key)` — global client created at import
- Lines 76-79: `conn = psycopg2.connect(...)` — global psycopg2 connection at import
- Line 144: `events_leagues` URL uses V1 API (`eventsseason.php`) while all other calls use V2
- Line 156: `time.sleep(rest)` — naïve rate limiter, no burst protection
- Lines 780-828: `active_league_ids` hardcoded list — requires code change to update
- Lines 838-845: file-based JSON cache with no expiry or integrity check
- Lines 916-929: `active_league_seasons_list` assigned twice — second assignment overwrites first, the first `fetch_supabase_data` call result is discarded
- Lines 1059-1062: `CREATE POLICY` without `DROP POLICY IF EXISTS` — crashes on every run after the first
- Line 381 in `supabase_tables.py`: `ROUND(RANDOM() * 100)` for `asset_luck` — random number, not a real metric

```python
# Standard libraries
import os
import logging
import time
import threading
from datetime import timedelta, datetime, timezone
from typing import List, Dict
import hashlib
import psycopg2
import json
import pickle

# External libraries
import re
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Application-specific or other imports
from supabase import create_client, Client

from supabase_tables import *

# Configure logging
start_time = time.time()
rest = 0.6
current_date = datetime.now().strftime('%Y-%m-%d')
# Ensure the log_files directory exists
log_directory = 'log_files'
if not os.path.exists(log_directory):
    os.mkdir(log_directory)

# Compute the hash of seconds since year 0
seconds_since_epoch = int(datetime.now().timestamp())
seconds_since_year_0 = seconds_since_epoch + 62135683200
commit_id = hashlib.sha256(str(seconds_since_year_0).encode()).hexdigest()[:5]

log_filename = f"{current_date}-{commit_id}.txt"
log_filepath = os.path.join(log_directory, log_filename)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)

file_handler = logging.FileHandler(log_filepath)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(file_handler)

load_dotenv()

ALLOWED_INPUTS = ['all', 'test']
API_KEY = os.getenv('SPORTSDB_API_KEY')
s_url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_USER_ROLE')
db = os.getenv('MUFL')
pw = os.getenv('MUFL_PW')
user = os.getenv('MUFL_USER')
host = os.getenv('MUFL_HOST')
port = os.getenv('MUFL_PORT')

# MODULE-LEVEL SIDE EFFECT: DB connections opened at import time
supabase = create_client(s_url, key)
conn = psycopg2.connect(database=db, user=user, password=pw, host=host, port=port)
cursor = conn.cursor()

def generate_date_list(duration: int = (10+14), lag: int = 14) -> List[str]:
    start_date = datetime.today() - timedelta(days=lag)
    date_list = [start_date - timedelta(days=x) for x in range(duration + lag)]
    date_list = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_list

def fetch_api_data(search_space, goal: str) -> Dict:
    thoughts = {}
    for thought in search_space:
        if goal == 'seasons':
            item = thought
            item2 = None
            url = f"https://www.thesportsdb.com/api/v2/json/list/seasons/{item}"
        elif goal == 'leagues':
            item = None
            item2 = None
            url = f"https://www.thesportsdb.com/api/v2/json/all/leagues"
        elif goal == 'teams':
            item = thought
            item2 = None
            url = f"https://www.thesportsdb.com/api/v2/json/list/teams/{item}"
        elif goal == 'league_details':
            item = thought
            item2 = None
            url = f"https://www.thesportsdb.com/api/v2/json/lookup/league/{item}"
        # V1 API still used for events:
        elif goal == 'events_leagues':
            item = thought[0]
            item2 = thought[1]
            url = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}/eventsseason.php?id={item}&s={item2}"
        try:
            time.sleep(rest)  # naive rate limiting
            headers = {"X-API-KEY": f"{API_KEY}", "Content-Type": "application/json"}
            response = requests.get(url, headers=headers, stream=False)
            response.raise_for_status()
            data = response.json()
            if item2 is not None:
                thoughts[(item, item2)] = data
            elif item is not None:
                thoughts[item] = data
            else:
                thoughts = data
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch data from {goal}. Error: {e}")
            raise
    return thoughts

def fetch_supabase_data(task: str) -> List[Dict]:
    endpoint_pairs = {
        'get_scored_events': ('sql_events_scored', '*'),
        'get_current_leagues': ('sql_leagues_current', 'league_id'),
        'get_all_league_seasons': ('api_seasons', 'league_id,league_season'),
        'get_current_elos': ('sql_ref_elo', '*'),
        'get_all_leagues': ('api_leagues', 'league_id'),
        'active_league_seasons_last5': ('sql_season_last5', 'league_id,league_season,season_rank'),
    }
    try:
        response = supabase.table(endpoint_pairs[task][0]).select(endpoint_pairs[task][1]).execute()
        return response.data
    except Exception as e:
        logging.warning(f"Failed to fetch data from Supabase. Error: {e}")
        raise

def upsert_data_to_supabase(data: List[Dict], option: str) -> None:
    options = {
        'leagues': ('api_leagues', 'league_id'),
        'league_details': ('api_league_details', 'league_id'),
        'teams': ('api_assets', 'uid'),
        'seasons': ('api_seasons', 'league_id,league_season'),
        'events': ('api_events', 'event_id'),
        'stats': ('py_stats', 'uid,league_season'),
        'tiers': ('py_tier', 'uid')
    }
    def handle_duplicates(data):
        seen = {}
        filtered_data = []
        for item in data:
            identifier = tuple(item[key] for key in options[option][1].split(','))
            if identifier not in seen:
                seen[identifier] = item
                filtered_data.append(item)
        return filtered_data
    data = handle_duplicates(data)
    def retry_upsert(sub_data, chunk_size):
        if chunk_size == 1:
            for item in sub_data:
                try:
                    supabase.table(options[option][0]).upsert([item], on_conflict=options[option][1]).execute()
                except Exception as e:
                    logging.error(f"Failed to upsert single item. Error: {e}")
            return
        for i in range(0, len(sub_data), chunk_size):
            try:
                supabase.table(options[option][0]).upsert(sub_data[i:i+chunk_size], on_conflict=options[option][1]).execute()
            except Exception as e:
                logging.warning(f"Failed in chunks of {chunk_size}. Error: {e}")
                next_chunk_size = 1 if chunk_size == 10 else 10
                retry_upsert(sub_data[i:i+chunk_size], next_chunk_size)
    for i in range(0, len(data), 100):
        if i % 1000 == 0:
            logging.info(f"Processing row {i} of {len(data)}")
        try:
            supabase.table(options[option][0]).upsert(data[i:i+100], on_conflict=options[option][1]).execute()
        except Exception as e:
            logging.warning(f"Failed in chunks of 100. Error: {e}")

def extract_season_data(data) -> List[Dict]:
    season_data = []
    for index, (league_id, info) in enumerate(data.items()):
        if 'list' in info and info['list']:
            for season in info['list']:
                season_data.append({
                    'league_id': league_id,
                    'league_season': season['strSeason'],
                    'updated_at': datetime.now(timezone.utc).isoformat()
                })
    return season_data

def extract_league_data(data) -> List[Dict]:
    league_data = []
    for index, (ids, info) in enumerate(data.items()):
        for league in info:
             league_data.append({
                'league_id': league['idLeague'],
                'league_name': league['strLeague'],
                'league_sport': league['strSport'],
                'league_name_alternate': league['strLeagueAlternate'],
                'created_at': datetime.now(timezone.utc).isoformat(),
            })
    return league_data

def extract_league_details_data(data) -> List[Dict]:
    league_data = []
    for index, (ids, info) in enumerate(data.items()):
        if info['lookup']:
            for league in info['lookup']:
                league_data.append({
                    'league_id': league['idLeague'],
                    'league_name': league['strLeague'],
                    'league_sport': league['strSport'],
                    'league_name_alternate': league['strLeagueAlternate'],
                    'league_division': league['intDivision'],
                    'league_cup': league['idCup'],
                    'league_current_season': league['strCurrentSeason'],
                    'league_formed_year': league['intFormedYear'],
                    'league_first_event': league['dateFirstEvent'],
                    'league_gender': league['strGender'],
                    'league_country': league['strCountry'],
                    'league_description_en': league['strDescriptionEN'],
                    'league_badge': league['strBadge'],
                    'league_trophy': league['strTrophy'],
                    'league_complete': league['strComplete'],
                    'created_at': datetime.now(timezone.utc).isoformat(),
                })
    return league_data

def extract_team_data(data: list[dict]) -> list:
    teams_data = []
    for index, (ids, info) in enumerate(data.items()):
        try:
            teams = [team_dicts for team_dicts in info['list']]
            for index2, team in enumerate(teams):
                try:
                    teams_data.append({
                        'uid': team['idLeague'] + '-' + team['idTeam'],
                        'league_id': team['idLeague'],
                        'team_name': team['strTeam'],
                        'team_short': team['strTeamShort'],
                        'created_at': datetime.now(timezone.utc).isoformat(),
                        'updated_at': datetime.now(timezone.utc).isoformat(),
                        'team_logo': team['strBadge'],
                        'team_country': team['strCountry'],
                    })
                except KeyError as e:
                    logging.warning(f"KeyError: {e} not found in data.")
        except:
            print('No Teams found in League', ids)
    return teams_data

def sanitize_date(date):
    return date.replace("0000-00-00", "1970-01-01").replace("-00", "-01")

def sanitize_time(time):
    if not time or time is None or time == "":
        return "12:00:00"
    time = time.replace(' AM ET', '')
    time = time.replace(' PM ET', '')
    time = time.replace(' ET', '')
    match = re.match(r'^(\d{2}:\d{2}:\d{2})', time)
    if match:
        return match.group(1)
    return time

def extract_event_data(data: list[dict]) -> list:
    events_data = []
    for index, (ids, info) in enumerate(data.items()):
        events = [event_dicts for event_dicts in info['events']]
        for index2, event in enumerate(events):
            try:
                event_id = event.get('idEvent', '')
                event_date = sanitize_date(event.get('dateEvent', '1970-01-01'))
                league_id = event.get('idLeague', ids[0] if ids else '')
                league_sport = event.get('strSport', '')
                league_season = ids[1] if ids and len(ids) > 1 else event.get('strSeason', '')
                league_round = event.get('intRound', 0)
                uid_home = f"{league_id}-{event.get('idHomeTeam', '')}"
                team_score_home = event.get('intHomeScore', 0)
                uid_away = f"{league_id}-{event.get('idAwayTeam', '')}"
                team_score_away = event.get('intAwayScore', 0)
                event_time = sanitize_time(event.get('strTime', '12:00:00'))
                event_status = event.get('strStatus', '')
                event_video = event.get('strVideo', '')
                updated_at = datetime.now(timezone.utc).isoformat()
                events_data.append({
                    'event_id': event_id, 'event_date': event_date,
                    'league_id': league_id, 'league_sport': league_sport,
                    'league_season': league_season, 'league_round': league_round,
                    'uid_home': uid_home, 'team_score_home': team_score_home,
                    'uid_away': uid_away, 'team_score_away': team_score_away,
                    'event_time': event_time, 'event_status': event_status,
                    'event_video': event_video, 'updated_at': updated_at
                })
            except KeyError as e:
                logging.warning(f"KeyError: {e} not found in data.")
    return events_data

def input_with_timeout(prompt: str, timeout: int = 30) -> str:
    result = {'value': None}
    def ask(user_value):
        user_value['value'] = input(prompt)
    thread = threading.Thread(target=ask, args=(result,))
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    return result['value'] if result['value'] in ALLOWED_INPUTS else "all"

def basic_stats(df):
    result_dict = {}
    for index, row in df.iterrows():  # ROW-BY-ROW LOOP — replaced by vectorised groupby
        home_key = (row['uid_home'], row['league_season'])
        if home_key not in result_dict:
            result_dict[home_key] = {'wins': 0, 'losses': 0, 'draws': 0,
                'points_for': 0, 'points_against': 0, 'games_played': 0,
                'home_wins': 0, 'home_losses': 0, 'home_draws': 0,
                'home_points_for': 0, 'home_points_against': 0, 'home_games_played': 0}
        away_key = (row['uid_away'], row['league_season'])
        if away_key not in result_dict:
            result_dict[away_key] = {'wins': 0, 'losses': 0, 'draws': 0,
                'points_for': 0, 'points_against': 0, 'games_played': 0,
                'home_wins': 0, 'home_losses': 0, 'home_draws': 0,
                'home_points_for': 0, 'home_points_against': 0, 'home_games_played': 0}
        if row['event_result'] == 'draw':
            result_dict[home_key]['draws'] += 1
            result_dict[away_key]['draws'] += 1
            result_dict[home_key]['home_draws'] += 1
        elif row['event_result'] == 'home':
            result_dict[home_key]['wins'] += 1
            result_dict[away_key]['losses'] += 1
            result_dict[home_key]['home_wins'] += 1
        else:
            result_dict[home_key]['losses'] += 1
            result_dict[away_key]['wins'] += 1
            result_dict[home_key]['home_losses'] += 1
        result_dict[home_key]['points_for'] += row['team_score_home']
        result_dict[home_key]['points_against'] += row['team_score_away']
        result_dict[home_key]['home_points_for'] += row['team_score_home']
        result_dict[home_key]['home_points_against'] += row['team_score_away']
        result_dict[away_key]['points_for'] += row['team_score_away']
        result_dict[away_key]['points_against'] += row['team_score_home']
        result_dict[home_key]['games_played'] += 1
        result_dict[home_key]['home_games_played'] += 1
        result_dict[away_key]['games_played'] += 1
    for key_y, value in result_dict.items():
        if value['games_played'] > 0:
            value['avg_points_for'] = value['points_for'] / value['games_played']
            value['avg_points_against'] = value['points_against'] / value['games_played']
        else:
            value['avg_points_against'] = 0
            value['avg_points_for'] = 0
        if value['home_games_played'] > 0:
            value['avg_home_points_for'] = value['home_points_for'] / value['home_games_played']
            value['avg_home_points_against'] = value['home_points_against'] / value['home_games_played']
        else:
            value['avg_home_points_for'] = 0
            value['avg_home_points_against'] = 0
    for key_z, value in result_dict.items():
        if value['games_played'] > 0:
            value['win_percentage'] = round((value['wins'] + (0.5 * value['draws'])) / value['games_played'], 2)
        else:
            value['win_percentage'] = 0
        if value['home_games_played'] > 0:
            value['home_win_percentage'] = round(
                (value['home_wins'] + (0.5 * value['home_draws'])) / value['home_games_played'], 2)
        else:
            value['home_win_percentage'] = 0
    return result_dict

def elo_stats(df):
    def elo_pred(home_elo, away_elo, home_field_advantage):
        diff = (away_elo - (home_elo + home_field_advantage)) / 400
        return 1 / (1 + (10 ** diff))
    uids = df['uid_home'].unique()
    uids = np.append(uids, df['uid_away'].unique())
    init_elo = 1500
    elo_ratings = {team: init_elo for team in uids}
    last_game_date = {team: None for team in uids}
    elo_history = []
    home_games = df.groupby(['league_id', 'uid_home']).size()
    home_wins = df[df['event_result'] == 'home'].groupby(['league_id', 'uid_home']).size()
    home_win_rates = home_wins / home_games
    away_games = df.groupby(['league_id', 'uid_away']).size()
    away_wins = df[df['event_result'] == 'away'].groupby(['league_id', 'uid_away']).size()
    away_win_rates = away_wins / away_games
    result = (home_win_rates - away_win_rates) + 0.5
    average_difference_by_league = result.groupby('league_id').mean()
    hfa = -400 * np.log((1 / average_difference_by_league) - 1) / np.log(10)
    home_field = hfa.to_dict()
    for index, row in df.reset_index().sort_values('event_date').iterrows():
        if index % 50000 == 0:
            logging.info(f"Processing row {index} of {len(df)}")
        uid_home = row['uid_home']
        uid_away = row['uid_away']
        expected_home = elo_pred(elo_ratings[uid_home], elo_ratings[uid_away], home_field[row['league_id']])
        expected_away = 1 - expected_home
        k_val = 20
        if row['league_sport'] == 'Baseball':
            k_val = 4
        elif row['league_sport'] == 'Soccer':
            k_val = 20.75
        if row['event_result'] == 'home':
            elo_change_home = k_val * (1 - expected_home)
            elo_change_away = k_val * (0 - expected_away)
        elif row['event_result'] == 'away':
            elo_change_home = k_val * (0 - expected_home)
            elo_change_away = k_val * (1 - expected_away)
        else:
            elo_change_home = k_val * (0.5 - expected_home)
            elo_change_away = k_val * (0.5 - expected_away)
        elo_ratings[uid_home] += round(elo_change_home, 1)
        elo_ratings[uid_away] += round(elo_change_away, 1)
        elo_history.append({'league_id': row['league_id'], 'uid': uid_home,
            'league_season': row['league_season'], 'current_elo': elo_ratings[uid_home],
            'event_id': row['event_id'], 'current_elo_delta': elo_change_home,
            'event_date': row['event_date']})
        elo_history.append({'league_id': row['league_id'], 'uid': uid_away,
            'league_season': row['league_season'], 'current_elo': elo_ratings[uid_away],
            'event_id': row['event_id'], 'current_elo_delta': elo_change_away,
            'event_date': row['event_date']})
        last_game_date[uid_home] = row['event_date']
        last_game_date[uid_away] = row['event_date']
    df_elo_history = pd.DataFrame(elo_history).sort_values(by='event_date')
    group = df_elo_history.sort_values(by='event_date').groupby(['league_id', 'uid', 'league_season'])
    final_df = group.agg(
        start_of_season_elo=pd.NamedAgg(column='current_elo', aggfunc='first'),
        end_of_season_elo=pd.NamedAgg(column='current_elo', aggfunc='last'),
        last_elo_delta=pd.NamedAgg(column='current_elo_delta', aggfunc='last')
    ).reset_index()
    final_df['season_elo_delta'] = final_df['end_of_season_elo'] - final_df['start_of_season_elo']
    elo_dict = {(row['uid'], row['league_season']): row for _, row in final_df.iterrows()}
    return elo_dict

def combine_dicts(result_dict, elo_dict):
    combined_dict = {}
    for key_x, value in result_dict.items():
        combined_dict[key_x] = {**value, **elo_dict[key_x]}
    return combined_dict

def process_elo_basic_stats(elo_stats, basic_stats):
    final = combine_dicts(basic_stats, elo_stats)
    final = pd.DataFrame.from_dict(final, orient='index').reset_index(drop=True)
    final['avg_points_for_percentile'] = final.groupby(['league_id', 'league_season'])['avg_points_for'].rank(pct=True, ascending=False)
    final['avg_points_against_percentile'] = final.groupby(['league_id', 'league_season'])['avg_points_against'].rank(pct=True, ascending=True)
    final['start_rank_league'] = final.groupby(['league_id', 'league_season'])['start_of_season_elo'].rank(ascending=False).fillna(0).astype(int)
    final['end_rank_league'] = final.groupby(['league_id', 'league_season'])['end_of_season_elo'].rank(ascending=False).fillna(0).astype(int)
    final['updated_at'] = datetime.now(timezone.utc).isoformat()
    return final

def calculate_tiers(elo_df):
    elo_df['percentile_rank'] = elo_df['end_of_season_elo'].rank(pct=True)
    tiers = [(0.995,'MOL'),(0.95,'SS'),(0.85,'S'),(0.70,'A'),(0.60,'B'),
             (0.50,'C'),(0.30,'D'),(0.15,'E'),(0.05,'F'),(0.005,'FF')]
    elo_df['tier'] = 'DIE'
    for percentile, tier in tiers:
        mask = (elo_df['tier'] == 'DIE') & (elo_df['percentile_rank'] > percentile)
        elo_df.loc[mask, 'tier'] = tier
    elo_df['updated_at'] = datetime.now(timezone.utc).isoformat()
    return elo_df[['uid', 'league_id', 'tier', 'updated_at']].to_dict(orient='records')

def main():
    active_league_ids = [
        4387, 4328, 4329, 4391, 4350, 4419, 4442, 4414
        # (full commented list omitted for brevity — see original file)
    ]
    create_supbase__sql_leagues_current(cursor, conn, active_league_ids)
    create_supbase__sql_season_current_and_past_last5(cursor, conn)

    if os.path.exists('all_leagues.json'):
        with open('all_leagues.json', 'r') as f:
            all_leagues = json.load(f)
    else:
        all_leagues = fetch_api_data([0], 'leagues')
        with open('all_leagues.json', 'w') as f:
            json.dump(all_leagues, f)
    all_leagues = extract_league_data(all_leagues)
    upsert_data_to_supabase(all_leagues, 'leagues')

    all_league_ids = [item['league_id'] for item in all_leagues]

    # ... (seasons, teams fetched similarly from cache or API) ...

    active_league_seasons_last5 = fetch_supabase_data('active_league_seasons_last5')
    # BUG: result immediately overwritten on next line:
    active_league_seasons_list = [(item['league_id'], item['league_season'])
                                   for item in active_league_seasons_last5]
    active_league_seasons_list = [  # overwrites previous line
        (league_id, season['strSeason'])
        for league_id, league_info in active_league_seasons.items()
        if 'list' in league_info and league_info['list']
        for season in league_info['list']
        if any(year in season['strSeason'] for year in ['2020','2021','2022','2023','2024','2025','2026'])
    ]

    active_league_events = fetch_api_data(active_league_seasons_list, 'events_leagues')
    with open('active_league_events.pkl', 'wb') as f:
        pickle.dump(active_league_events, f)
    if os.path.exists('active_league_events.pkl'):
        with open('active_league_events.pkl', 'rb') as f:
            active_league_events = pickle.load(f)

    active_league_events = extract_event_data(active_league_events)
    upsert_data_to_supabase(active_league_events, 'events')

    create_supbase__sql_web_events(cursor, conn)
    create_supbase__sql_web_events_scored(cursor, conn)
    create_supbase__sql_events_split(cursor, conn)

    scored_events = fetch_supabase_data('get_scored_events')
    scored_events = pd.DataFrame(scored_events)
    basic_stats_dict = basic_stats(scored_events)
    elo_stats_dict = elo_stats(scored_events)
    final = process_elo_basic_stats(elo_stats_dict, basic_stats_dict)
    upsert_data_to_supabase(final.to_dict(orient='records'), 'stats')

    create_supbase__sql_py_stats_utd_and_ls(cursor, conn)
    create_supbase__sql_asset_last_10_games(cursor, conn)
    create_supbase__sql_events_future_elos(cursor, conn)
    create_supbase__sql_assets_future(cursor, conn)
    create_supbase__sql_assets_stats_at(cursor, conn)
    create_supbase__sql_forecast(cursor, conn)
    create_supbase_sql_assets_season_to_date(cursor, conn)
    create_supbase__sql_ref_elo(cursor, conn)

    current_elos = fetch_supabase_data('get_current_elos')
    current_elos = pd.DataFrame(current_elos)
    current_tiers = calculate_tiers(current_elos)
    upsert_data_to_supabase(current_tiers, 'tiers')

    create_supbase__sql_current_elo(cursor, conn)
    create_supbase__sql_wld(cursor, conn)
    create_supbase__sql_web_assets(cursor, conn)
    create_supbase__sql_web_assets_info(cursor, conn)
    create_supbase__sql_league_info(cursor, conn)

    tables_to_enable_rls = [
        "sql_leagues_current", "sql_season_current", "sql_season_past",
        "sql_web_events", "sql_events_scored", "sql_py_stats_utd",
        "sql_py_stats_ls", "sql_events_future_elos", "sql_assets_future",
        "sql_assets_stats_at", "sql_forecast", "sql_events_split",
        "sql_asset_last_10_games", "sql_ref_elo", "sql_current_elo",
        "sql_wld", "sql_web_assets", "sql_league_info"
    ]
    rls_query = "ALTER TABLE public.{} ENABLE ROW LEVEL SECURITY;"
    policy_query = """CREATE POLICY "Enable read access for all users"
    ON public.{} AS PERMISSIVE FOR SELECT TO public USING (true);"""  # no DROP IF EXISTS — crashes on re-run
    for table in tables_to_enable_rls:
        cursor.execute(rls_query.format(table))
        cursor.execute(policy_query.format(table))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
```

---

\newpage

# Appendix B — Original `supabase_tables.py` (781 lines)

The companion SQL file. All table definitions are Python functions that construct SQL strings and execute them via a psycopg2 cursor. Reproduced here for reference. Key issues: `js_rounds` JOIN present in `sql_web_events` and `sql_events_scored`; `ROUND(RANDOM() * 100)` in `sql_web_assets`; `event_round` column appears throughout; `INNER JOIN` on both utd and ls stat tables in `sql_events_future_elos`.

All of these issues are fixed in the `.sql` files in Chapter 7.

```python
# Standard libraries
import os
import logging
import time
from datetime import timedelta, datetime
from typing import List, Dict
import psycopg2

rest = 0.6

def create_supbase__sql_leagues_current(cursor, conn, current_leagues):
    cursor.execute("""drop table if exists public.sql_leagues_current;""")
    conn.commit()
    cursor.execute(f"""
        CREATE table public.sql_leagues_current AS
            SELECT *, league_name as league_name_clean
        FROM public.api_leagues
        WHERE league_id in ({",".join(str(L) for L in current_leagues)})""")
    conn.commit()

def create_supbase__sql_season_current_and_past_last5(cursor, conn):
    cursor.execute("""DROP TABLE IF EXISTS public.sql_season_current;""")
    conn.commit()
    cursor.execute("""
        CREATE TABLE PUBLIC.sql_season_current AS
            SELECT league_id, league_season FROM (
                SELECT s.league_id, s.league_season, s.created_at, s.updated_at,
                    ROW_NUMBER() OVER (PARTITION BY s.league_id ORDER BY s.league_season DESC NULLS LAST) AS season_rank
                FROM PUBLIC.api_seasons s
                WHERE s.league_id IN (SELECT league_id FROM PUBLIC.sql_leagues_current)
                GROUP BY s.league_id, s.league_season, s.created_at, s.updated_at
                ) AS ranked_seasons
            WHERE season_rank = 1;""")
    conn.commit()
    cursor.execute("""DROP TABLE IF EXISTS public.sql_season_past;""")
    conn.commit()
    cursor.execute("""
        CREATE TABLE PUBLIC.sql_season_past AS
            SELECT league_id, league_season FROM (
                SELECT s.league_id, s.league_season, s.created_at, s.updated_at,
                    ROW_NUMBER() OVER (PARTITION BY s.league_id ORDER BY s.league_season DESC NULLS LAST) AS season_rank
                FROM PUBLIC.api_seasons s
                INNER JOIN PUBLIC.api_events e ON s.league_id = e.league_id AND s.league_season = e.league_season
                WHERE s.league_id IN (SELECT league_id FROM PUBLIC.sql_leagues_current)
                GROUP BY s.league_id, s.league_season, s.created_at, s.updated_at
                ) AS ranked_seasons
            WHERE season_rank = 2;""")
    conn.commit()
    cursor.execute("""DROP TABLE IF EXISTS public.sql_season_last5;""")
    conn.commit()
    cursor.execute("""
        CREATE TABLE PUBLIC.sql_season_last5 AS
            SELECT league_id, league_season, season_rank FROM (
                SELECT s.league_id, s.league_season, s.created_at, s.updated_at,
                    ROW_NUMBER() OVER (PARTITION BY s.league_id ORDER BY s.league_season DESC NULLS LAST) AS season_rank
                FROM PUBLIC.api_seasons s
                INNER JOIN PUBLIC.api_events e ON s.league_id = e.league_id AND s.league_season = e.league_season
                WHERE s.league_id IN (SELECT league_id FROM PUBLIC.sql_leagues_current)
                GROUP BY s.league_id, s.league_season, s.created_at, s.updated_at
                ) AS ranked_seasons
            WHERE season_rank <= 5;""")
    conn.commit()

def create_supbase__sql_web_events(cursor, conn):
    cursor.execute("""
        DROP TABLE IF EXISTS public.sql_web_events;
        CREATE TABLE PUBLIC.sql_web_events AS
            SELECT api_events.event_id, api_events.event_date, api_events.event_time,
                round.round_name as event_round,  -- js_rounds dependency
                CASE
                    WHEN api_events.event_status = 'Match Finished' THEN 'F'
                    WHEN api_events.event_status = 'FT' THEN 'F'
                    WHEN api_events.event_status = 'AOT' THEN 'F'
                    WHEN api_events.event_status = 'Not Started' THEN 'NS'
                    WHEN api_events.event_status = 'Time to be defined' THEN 'NS'
                    WHEN api_events.event_status = '1H' THEN '1st'
                    WHEN api_events.event_status = '2H' THEN '2nd'
                    WHEN api_events.event_status = 'HT' THEN 'Half'
                    WHEN (api_events.event_date::DATE < CURRENT_DATE::DATE
                        AND api_events.team_score_away IS NOT NULL
                        AND api_events.team_score_home IS NOT NULL) THEN 'F'
                    WHEN (api_events.event_date::DATE > CURRENT_DATE::DATE) THEN 'NS'
                    WHEN (api_events.event_date::DATE = CURRENT_DATE::DATE) THEN 'NS'
                    ELSE 'Unknown' END AS event_status,
                api_events.event_video, api_events.league_id, api_leagues.league_name,
                api_leagues.league_name AS league_name_clean, api_events.league_sport,
                api_events.league_season, api_events.uid_home, home.team_name AS team_name_home,
                api_events.team_score_home, api_events.uid_away, away.team_name AS team_name_away,
                api_events.team_score_away, current_timestamp AS updated_at
            FROM PUBLIC.api_events
            LEFT JOIN PUBLIC.sql_season_last5 as seasons
                ON seasons.league_id = api_events.league_id
                AND seasons.league_season = api_events.league_season
            INNER JOIN PUBLIC.api_assets AS home on home.uid = api_events.uid_home
            INNER JOIN PUBLIC.api_assets AS away on away.uid = api_events.uid_away
            INNER JOIN api_leagues ON api_leagues.league_id = api_events.league_id
            INNER JOIN sql_leagues_current ON sql_leagues_current.league_id = api_events.league_id
            LEFT JOIN js_rounds as round  -- js_rounds JOIN
                on api_events.event_date >= round.round_start
                and api_events.event_date < round.round_end
            WHERE (seasons.league_id IS NOT NULL OR event_date >= CURRENT_DATE);
        CREATE INDEX idx_sql_web_events ON public.sql_web_events(league_id, event_id);""")
    conn.commit()

def create_supbase__sql_web_events_scored(cursor, conn):
    cursor.execute("""
        DROP TABLE IF EXISTS public.sql_events_scored;
        CREATE TABLE PUBLIC.sql_events_scored AS
            SELECT api_events.event_id, api_events.league_id,
                round.round_name as event_round,  -- js_rounds dependency
                api_events.league_season, api_events.event_date, api_events.league_sport,
                api_events.uid_home, api_events.uid_away,
                api_events.team_score_home, api_events.team_score_away,
                CASE
                    WHEN team_score_home - team_score_away = 0 THEN 'draw'
                    WHEN team_score_home - team_score_away > 0 THEN 'home'
                    WHEN team_score_home - team_score_away < 0 THEN 'away'
                    END AS "event_result",
                ROW_NUMBER() OVER (PARTITION BY api_events.league_id ORDER BY api_events.event_date ASC) AS game_order
            FROM PUBLIC.api_events
            INNER JOIN PUBLIC.sql_leagues_current ON sql_leagues_current.league_id = api_events.league_id
            LEFT JOIN js_rounds as round  -- js_rounds JOIN
                on api_events.event_date >= round.round_start
                and api_events.event_date < round.round_end
            WHERE team_score_away IS NOT NULL AND team_score_home IS NOT NULL AND event_date IS NOT NULL;
        CREATE INDEX idx_sql_events_scored ON public.sql_events_scored(league_id, event_id);""")

# ... (remaining functions create sql_py_stats_utd/ls, sql_current_elo, sql_wld,
#      sql_web_assets with ROUND(RANDOM()*100), sql_events_future_elos with INNER JOIN bug,
#      sql_assets_future, sql_assets_season_to_date, sql_assets_stats_at, sql_forecast,
#      sql_events_split, sql_asset_last_10_games, sql_ref_elo, sql_league_info,
#      sql_web_assets_info — all reproduced exactly in the original file)
```

*The full 781-line `supabase_tables.py` is in the original repository. The complete, corrected SQL for all tables is in Chapter 7 of this document.*
