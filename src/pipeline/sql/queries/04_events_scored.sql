DROP TABLE IF EXISTS derived.events_scored CASCADE;

CREATE TABLE derived.events_scored AS
    SELECT
        e.event_id
        , e.league_id
        , e.league_season
        , e.event_date
        , e.league_sport
        , e.uid_home
        , e.uid_away
        , e.team_score_home
        , e.team_score_away
        , CASE
            WHEN e.team_score_home - e.team_score_away = 0 THEN 'draw'
            WHEN e.team_score_home - e.team_score_away > 0 THEN 'home'
            WHEN e.team_score_home - e.team_score_away < 0 THEN 'away'
          END                                               AS event_result
        , ROW_NUMBER() OVER (
            PARTITION BY e.league_id
            ORDER BY e.event_date ASC, e.event_id ASC
          )                                                 AS game_order
    FROM api.events AS e
    INNER JOIN derived.leagues_current AS lc
        ON lc.league_id = e.league_id
    WHERE
        e.team_score_away IS NOT NULL
        AND e.team_score_home IS NOT NULL
        AND e.event_date IS NOT NULL;

CREATE INDEX idx_events_scored
    ON derived.events_scored (league_id, event_id);
