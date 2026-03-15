DROP TABLE IF EXISTS derived.web_events CASCADE;

CREATE TABLE derived.web_events AS
    SELECT
        e.event_id
        , e.event_date
        , e.event_time
        , CASE
            WHEN e.event_status = 'Match Finished'           THEN 'F'
            WHEN e.event_status IN ('FT', 'AOT')             THEN 'F'
            WHEN e.event_status = 'Not Started'              THEN 'NS'
            WHEN e.event_status = 'Time to be defined'       THEN 'NS'
            WHEN e.event_status = '1H'                       THEN '1st'
            WHEN e.event_status = '2H'                       THEN '2nd'
            WHEN e.event_status = 'HT'                       THEN 'Half'
            WHEN (
                e.event_date::DATE < CURRENT_DATE::DATE
                AND e.team_score_away IS NOT NULL
                AND e.team_score_home IS NOT NULL
            )                                                THEN 'F'
            WHEN e.event_date::DATE > CURRENT_DATE::DATE     THEN 'NS'
            WHEN e.event_date::DATE = CURRENT_DATE::DATE     THEN 'NS'
            ELSE 'Unknown'
          END                                               AS event_status
        , e.event_video
        , e.league_id
        , al.league_name
        , al.league_name                                    AS league_name_clean
        , e.league_sport
        , e.league_season
        , e.uid_home
        , home.team_name                                    AS team_name_home
        , e.team_score_home
        , e.uid_away
        , away.team_name                                    AS team_name_away
        , e.team_score_away
        , CURRENT_TIMESTAMP                                 AS updated_at
    FROM api.events AS e
    LEFT JOIN derived.season_last5 AS seasons
        ON seasons.league_id    = e.league_id
       AND seasons.league_season = e.league_season
    INNER JOIN api.assets AS home
        ON home.uid = e.uid_home
    INNER JOIN api.assets AS away
        ON away.uid = e.uid_away
    INNER JOIN api.leagues AS al
        ON al.league_id = e.league_id
    INNER JOIN derived.leagues_current AS lc
        ON lc.league_id = e.league_id
    WHERE (
        seasons.league_id IS NOT NULL
        OR e.event_date >= CURRENT_DATE
    );

CREATE INDEX idx_web_events
    ON derived.web_events (league_id, event_id);
