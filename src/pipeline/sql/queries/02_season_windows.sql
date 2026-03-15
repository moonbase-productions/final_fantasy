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
        GROUP BY s.league_id, s.league_season
    ) AS ranked
    WHERE season_rank <= 5;

CREATE INDEX idx_season_last5
    ON derived.season_last5 (league_id, league_season);
