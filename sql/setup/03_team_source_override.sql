-- =============================================================================
-- Migration: add team_source_league_id override to admin.league_registry.
--
-- Run this once in the Supabase SQL Editor against an existing database
-- (one that already ran 01_permanent_tables.sql + 02_league_registry.sql
-- before this column existed). Safe to re-run — IF NOT EXISTS guards it.
--
-- On a brand-new setup, 02_league_registry.sql already creates this
-- column directly, so this file is a no-op there.
--
-- See 02_league_registry.sql for the column's purpose/comment.
-- =============================================================================

ALTER TABLE admin.league_registry
    ADD COLUMN IF NOT EXISTS team_source_league_id TEXT;
