DROP TABLE IF EXISTS derived.forecast CASCADE;

CREATE TABLE derived.forecast AS
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
        FROM derived.assets_future
    )
    SELECT
        uid
        , league_id
        , AVG(team_home_win_prob)   AS avg_win_probability_next_20_games
    FROM ranked
    WHERE rn BETWEEN 1 AND 20
    GROUP BY uid, league_id;

CREATE INDEX idx_forecast
    ON derived.forecast (uid);
