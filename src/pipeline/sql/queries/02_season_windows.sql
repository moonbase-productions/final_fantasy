DROP TABLE IF EXISTS derived.season_current CASCADE;

CREATE TABLE derived.season_current AS
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
        FROM api.seasons AS s
        WHERE s.league_id IN (SELECT league_id FROM derived.leagues_current)
          -- Exclude phantom/placeholder seasons with zero events (e.g. TheSportsDB
          -- pre-registering next season's label before any games are scheduled).
          -- Left unfiltered, a season string that sorts after the real current
          -- season (e.g. "2026-2027" > "2026") would outrank it and corrupt
          -- season_current/season_past/season_last5 for the whole league.
          AND EXISTS (
              SELECT 1 FROM api.events AS e
              WHERE e.league_id     = s.league_id
                AND e.league_season = s.league_season
          )
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank = 1;

DROP TABLE IF EXISTS derived.season_past CASCADE;

CREATE TABLE derived.season_past AS
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
        FROM api.seasons AS s
        WHERE s.league_id IN (SELECT league_id FROM derived.leagues_current)
          -- Exclude phantom/placeholder seasons with zero events (e.g. TheSportsDB
          -- pre-registering next season's label before any games are scheduled).
          -- Left unfiltered, a season string that sorts after the real current
          -- season (e.g. "2026-2027" > "2026") would outrank it and corrupt
          -- season_current/season_past/season_last5 for the whole league.
          AND EXISTS (
              SELECT 1 FROM api.events AS e
              WHERE e.league_id     = s.league_id
                AND e.league_season = s.league_season
          )
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank = 2;

DROP TABLE IF EXISTS derived.season_last5 CASCADE;

CREATE TABLE derived.season_last5 AS
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
        FROM api.seasons AS s
        WHERE s.league_id IN (SELECT league_id FROM derived.leagues_current)
          -- Exclude phantom/placeholder seasons with zero events (e.g. TheSportsDB
          -- pre-registering next season's label before any games are scheduled).
          -- Left unfiltered, a season string that sorts after the real current
          -- season (e.g. "2026-2027" > "2026") would outrank it and corrupt
          -- season_current/season_past/season_last5 for the whole league.
          AND EXISTS (
              SELECT 1 FROM api.events AS e
              WHERE e.league_id     = s.league_id
                AND e.league_season = s.league_season
          )
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank <= 5;

CREATE INDEX idx_season_last5
    ON derived.season_last5 (league_id, league_season);
