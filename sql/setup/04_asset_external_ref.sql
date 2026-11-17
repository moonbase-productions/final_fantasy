-- Migration: add a generic external-source identifier to api.assets, and
-- create api.team_details if it doesn't already exist.
--
-- Run this once against the live database (Supabase SQL editor, or psql /
-- psycopg2 against the direct DB host — same as sql/setup/03_team_source_override.sql).

-- ---------------------------------------------------------------------------
-- api.assets.external_ref
--
-- For a custom (non-TheSportsDB) source, asset ids are just sequential
-- integers assigned by the pipeline (e.g. NCAA women's soccer, league 9000)
-- rather than TheSportsDB's own numeric team id. external_ref stores that
-- source's own stable identifier for the team (e.g. NCAA's "seo" slug, like
-- "duke") so a later incremental fetch can look up "have I already assigned
-- this team an id?" instead of reassigning a new one every run. NULL for
-- every TheSportsDB-sourced team (those already have a stable id: the
-- TheSportsDB team_id baked into the uid itself).
-- ---------------------------------------------------------------------------
ALTER TABLE api.assets ADD COLUMN IF NOT EXISTS external_ref TEXT;

COMMENT ON COLUMN api.assets.external_ref IS
    'Stable external identifier from a non-TheSportsDB data source (e.g. the NCAA team "seo" slug), used to keep this row''s integer id stable across incremental extractor runs for custom-sourced leagues. NULL for TheSportsDB-sourced teams.';

CREATE INDEX IF NOT EXISTS idx_api_assets_external_ref
    ON api.assets (league_id, external_ref)
    WHERE external_ref IS NOT NULL;

-- ---------------------------------------------------------------------------
-- api.team_details
--
-- Extended per-team profile info (venue, colors, socials, description, ...).
-- Schema mirrors TheSportsDB's team-lookup response shape. For
-- custom-sourced leagues (e.g. NCAA), most of these fields simply aren't
-- available and are left NULL rather than invented.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api.team_details (
    team_id             BIGINT PRIMARY KEY,
    id_espn             TEXT,
    id_api_football     TEXT,
    team_name           TEXT,
    team_alternate      TEXT,
    team_short          TEXT,
    formed_year         TEXT,
    sport               TEXT,
    league_name         TEXT,
    league_id           TEXT,
    league2_name        TEXT,
    league2_id          TEXT,
    league3_name        TEXT,
    league3_id          TEXT,
    league4_name        TEXT,
    league4_id          TEXT,
    league5_name        TEXT,
    league5_id          TEXT,
    league6_name        TEXT,
    league6_id          TEXT,
    league7_name        TEXT,
    league7_id          TEXT,
    division            TEXT,
    venue_id            TEXT,
    stadium             TEXT,
    stadium_capacity    TEXT,
    location            TEXT,
    website             TEXT,
    facebook            TEXT,
    twitter             TEXT,
    instagram           TEXT,
    youtube             TEXT,
    description_en      TEXT,
    colour1             TEXT,
    colour2             TEXT,
    colour3             TEXT,
    gender              TEXT,
    country             TEXT,
    badge               TEXT,
    logo                TEXT,
    fanart1             TEXT,
    fanart2             TEXT,
    fanart3             TEXT,
    fanart4             TEXT,
    banner              TEXT,
    equipment           TEXT,
    locked              TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    lat                 FLOAT,
    lon                 FLOAT
);

GRANT SELECT ON api.team_details TO anon, authenticated;
GRANT ALL    ON api.team_details TO service_role;
