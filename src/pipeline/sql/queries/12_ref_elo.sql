DROP TABLE IF EXISTS derived.ref_elo;

CREATE TABLE derived.ref_elo AS
    WITH ranked AS (
        SELECT
            ts.uid
            , ts.league_id
            , ts.league_season
            , ts.end_of_season_elo
            , ROW_NUMBER() OVER (
                PARTITION BY ts.uid
                ORDER BY ts.league_season DESC NULLS LAST
              ) AS rn
        FROM stats.team_stats AS ts
        INNER JOIN derived.leagues_current AS lc
            ON lc.league_id = ts.league_id
        INNER JOIN derived.season_last5 AS s5
            ON s5.league_id    = ts.league_id
           AND s5.league_season = ts.league_season
    )
    SELECT
        uid
        , league_id
        , league_season
        , end_of_season_elo
    FROM ranked
    WHERE rn = 1
    ORDER BY end_of_season_elo ASC;
