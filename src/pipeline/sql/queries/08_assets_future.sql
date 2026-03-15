DROP TABLE IF EXISTS derived.assets_future CASCADE;

CREATE TABLE derived.assets_future AS
    WITH combined AS (
        SELECT
            uid_home                AS uid
            , league_id
            , league_season
            , event_date
            , team_home_win_prob    AS team_home_win_prob
            , event_id
        FROM derived.events_future_elos

        UNION ALL

        SELECT
            uid_away                AS uid
            , league_id
            , league_season
            , event_date
            , team_away_win_prob    AS team_home_win_prob
            , event_id
        FROM derived.events_future_elos
    )
    SELECT * FROM combined;

CREATE INDEX idx_assets_future
    ON derived.assets_future (uid);
