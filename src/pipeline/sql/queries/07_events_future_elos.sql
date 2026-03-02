DROP TABLE IF EXISTS derived.team_stats_current;

CREATE TABLE derived.team_stats_current AS
    SELECT ts.*
    FROM stats.team_stats AS ts
    INNER JOIN derived.season_last5 AS s5
        ON s5.league_id    = ts.league_id
       AND s5.league_season = ts.league_season
    WHERE s5.season_rank = 1;

CREATE INDEX idx_team_stats_current
    ON derived.team_stats_current (uid);

DROP TABLE IF EXISTS derived.team_stats_previous;

CREATE TABLE derived.team_stats_previous AS
    SELECT ts.*
    FROM stats.team_stats AS ts
    INNER JOIN derived.season_last5 AS s5
        ON s5.league_id    = ts.league_id
       AND s5.league_season = ts.league_season
    WHERE s5.season_rank = 2;

CREATE INDEX idx_team_stats_previous
    ON derived.team_stats_previous (uid);

DROP TABLE IF EXISTS derived.events_future_elos;

CREATE TABLE derived.events_future_elos AS
    SELECT
        we.event_id
        , we.league_id
        , we.league_season
        , we.event_date
        , we.league_sport
        , we.uid_home
        , we.uid_away
        , we.team_score_home
        , we.team_score_away
        , ROW_NUMBER() OVER (
            PARTITION BY we.league_id
            ORDER BY we.event_date ASC
          )                                                         AS game_order
        , COALESCE(home_cur.end_of_season_elo,
                   home_prv.end_of_season_elo)                      AS team_elo_home
        , COALESCE(away_cur.end_of_season_elo,
                   away_prv.end_of_season_elo)                      AS team_elo_away
        , COALESCE(home_cur.home_field_advantage,
                   home_prv.home_field_advantage, 0)                AS hfa
        , 1.0 / (
            POWER(10,
                -(
                    (COALESCE(home_cur.end_of_season_elo, home_prv.end_of_season_elo)
                     + COALESCE(home_cur.home_field_advantage, home_prv.home_field_advantage, 0))
                  - COALESCE(away_cur.end_of_season_elo, away_prv.end_of_season_elo)
                ) / 400.0
            ) + 1
          )                                                         AS team_home_win_prob
        , 1.0 / (
            POWER(10,
                -(
                    COALESCE(away_cur.end_of_season_elo, away_prv.end_of_season_elo)
                  - (COALESCE(home_cur.end_of_season_elo, home_prv.end_of_season_elo)
                     + COALESCE(home_cur.home_field_advantage, home_prv.home_field_advantage, 0))
                ) / 400.0
            ) + 1
          )                                                         AS team_away_win_prob
    FROM derived.web_events AS we
    LEFT JOIN derived.team_stats_current AS home_cur
        ON home_cur.uid = we.uid_home
    LEFT JOIN derived.team_stats_current AS away_cur
        ON away_cur.uid = we.uid_away
    LEFT JOIN derived.team_stats_previous AS home_prv
        ON home_prv.uid = we.uid_home
    LEFT JOIN derived.team_stats_previous AS away_prv
        ON away_prv.uid = we.uid_away
    WHERE
        we.event_date >= CURRENT_DATE::DATE
        AND we.team_score_away IS NULL
        AND we.team_score_home IS NULL;
