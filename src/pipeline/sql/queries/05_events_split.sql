DROP TABLE IF EXISTS derived.events_split;

CREATE TABLE derived.events_split AS
    WITH cte AS (
        -- Home team perspective
        SELECT
            e.uid_home              AS uid
            , e.league_id
            , e.league_season
            , e.event_date
            , e.team_score_home     AS team_points_for
            , e.team_score_away     AS team_points_against
        FROM api.events AS e
        INNER JOIN derived.leagues_current AS lc
            ON lc.league_id = e.league_id
        WHERE
            e.team_score_away IS NOT NULL
            AND e.team_score_home IS NOT NULL

        UNION ALL

        -- Away team perspective
        SELECT
            e.uid_away              AS uid
            , e.league_id
            , e.league_season
            , e.event_date
            , e.team_score_away     AS team_points_for
            , e.team_score_home     AS team_points_against
        FROM api.events AS e
        INNER JOIN derived.leagues_current AS lc
            ON lc.league_id = e.league_id
        WHERE
            e.team_score_away IS NOT NULL
            AND e.team_score_home IS NOT NULL
    )
    SELECT * FROM cte;

CREATE INDEX idx_events_split
    ON derived.events_split (uid);
