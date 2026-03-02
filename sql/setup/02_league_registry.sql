-- =============================================================================
-- League registry — admin-controlled whitelist and active-league config.
-- Run after 01_permanent_tables.sql.
--
-- is_whitelisted: pipeline fetches data for this league (weekly full refresh)
-- is_active:      league appears on the fantasy platform (daily update)
-- sport_type:     controls event normalisation for Elo computation
--   'standard'          = home/away teams, numeric scores (soccer, basketball, …)
--   'binary'            = winner/loser only, no meaningful score (UFC, tennis, …)
--   'multi_competitor'  = multiple competitors per event (F1, NASCAR, cycling)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS admin;

GRANT USAGE ON SCHEMA admin TO anon, authenticated, service_role;

CREATE TABLE IF NOT EXISTS admin.league_registry (
    league_id       TEXT PRIMARY KEY,
    league_name     TEXT NOT NULL,
    league_sport    TEXT NOT NULL,
    sport_type      TEXT CHECK (sport_type IN ('standard', 'binary', 'multi_competitor')),
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

-- Public read; writes via service_role only
ALTER TABLE admin.league_registry ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "public_read" ON admin.league_registry;
CREATE POLICY "public_read" ON admin.league_registry
    FOR SELECT TO public USING (true);

GRANT SELECT ON admin.league_registry TO anon, authenticated;
GRANT ALL    ON admin.league_registry TO service_role;
