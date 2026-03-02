-- =============================================================================
-- Permanent tables for the pipeline.
-- Run once in Supabase SQL Editor before the first pipeline run.
--
-- After running this file, also run 02_league_registry.sql.
--
-- Then add these schemas to Supabase's Exposed Schemas list:
--   Dashboard → Settings → API → Exposed schemas
--   Add: api, stats, admin, derived
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS api;
CREATE SCHEMA IF NOT EXISTS stats;
CREATE SCHEMA IF NOT EXISTS derived;

-- Grant schema-level access for PostgREST roles
GRANT USAGE ON SCHEMA api,
               stats,
               derived
    TO anon, authenticated, service_role;

-- Default privileges so every future table in derived gets SELECT automatically
-- (derived tables are dropped & recreated on each pipeline run)
ALTER DEFAULT PRIVILEGES IN SCHEMA derived
    GRANT SELECT ON TABLES TO anon, authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA derived
    GRANT ALL ON TABLES TO service_role;


-- ---------------------------------------------------------------------------
-- api schema — raw data from TheSportsDB
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api.leagues (
    league_id               TEXT PRIMARY KEY,
    league_name             TEXT,
    league_sport            TEXT,
    league_name_alternate   TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api.league_details (
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

CREATE TABLE IF NOT EXISTS api.seasons (
    league_id       TEXT NOT NULL,
    league_season   TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (league_id, league_season)
);

-- assets = teams (called "assets" in the fantasy context)
-- uid = league_id + '-' + team_id  e.g. "4328-133604"
CREATE TABLE IF NOT EXISTS api.assets (
    uid             TEXT PRIMARY KEY,
    league_id       TEXT NOT NULL,
    team_name       TEXT,
    team_short      TEXT,
    team_logo       TEXT,
    team_country    TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- team_score_home/away are FLOAT to support normalised scores:
--   standard sports: raw numeric score
--   binary sports: 1.0 (win) / 0.5 (draw) / 0.0 (loss)
--   multi-competitor: championship points for finish position
CREATE TABLE IF NOT EXISTS api.events (
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
    ON api.events (league_id, league_season);
CREATE INDEX IF NOT EXISTS idx_api_events_date
    ON api.events (event_date);


-- ---------------------------------------------------------------------------
-- stats schema — Python-computed statistics
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stats.team_stats (
    uid                             TEXT NOT NULL,
    league_id                       TEXT NOT NULL,
    league_season                   TEXT NOT NULL,
    wins                            INT,
    losses                          INT,
    draws                           INT,
    points_for                      FLOAT,
    points_against                  FLOAT,
    games_played                    INT,
    avg_points_for                  FLOAT,
    avg_points_against              FLOAT,
    win_percentage                  FLOAT,
    home_wins                       INT,
    home_losses                     INT,
    home_draws                      INT,
    home_points_for                 FLOAT,
    home_points_against             FLOAT,
    home_games_played               INT,
    avg_home_points_for             FLOAT,
    avg_home_points_against         FLOAT,
    home_win_percentage             FLOAT,
    avg_points_for_percentile       FLOAT,
    avg_points_against_percentile   FLOAT,
    start_rank_league               INT,
    end_rank_league                 INT,
    start_of_season_elo             FLOAT,
    end_of_season_elo               FLOAT,
    last_elo_delta                  FLOAT,
    season_elo_delta                FLOAT,
    luck_score                      FLOAT,
    luck_display                    INT,
    updated_at                      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (uid, league_season)
);

CREATE INDEX IF NOT EXISTS idx_stats_team_stats_uid
    ON stats.team_stats (uid);

CREATE TABLE IF NOT EXISTS stats.team_tiers (
    uid         TEXT PRIMARY KEY,
    league_id   TEXT,
    tier        TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);


-- ---------------------------------------------------------------------------
-- Grants for permanent tables (read by anyone, write by service_role only)
-- ---------------------------------------------------------------------------

GRANT SELECT ON ALL TABLES IN SCHEMA api   TO anon, authenticated;
GRANT ALL    ON ALL TABLES IN SCHEMA api   TO service_role;

GRANT SELECT ON ALL TABLES IN SCHEMA stats TO anon, authenticated;
GRANT ALL    ON ALL TABLES IN SCHEMA stats TO service_role;
