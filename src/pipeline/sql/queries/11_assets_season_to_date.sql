DROP TABLE IF EXISTS derived.assets_season_to_date;

CREATE TABLE derived.assets_season_to_date AS
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
        FROM derived.events_split AS es
        INNER JOIN derived.season_last5 AS s5
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
            , CAST(1.0 * wins / (wins + losses + draws) AS FLOAT)       AS win_percentage
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

CREATE INDEX idx_assets_season_to_date
    ON derived.assets_season_to_date (uid, league_season);
