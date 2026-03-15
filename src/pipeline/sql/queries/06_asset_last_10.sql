DROP TABLE IF EXISTS derived.asset_last_10 CASCADE;

CREATE TABLE derived.asset_last_10 AS
    WITH last_10 AS (
        SELECT
            uid
            , league_id
            , event_date
            , team_points_for
            , team_points_against
            , ROW_NUMBER() OVER (
                PARTITION BY uid
                ORDER BY event_date DESC NULLS LAST
              )                                             AS rn
            , CASE
                WHEN team_points_for IS NULL
                  OR team_points_against IS NULL            THEN '?'
                WHEN team_points_for > team_points_against  THEN 'W'
                WHEN team_points_for < team_points_against  THEN 'L'
                WHEN team_points_for = team_points_against  THEN 'D'
              END                                           AS result
        FROM derived.events_split
    )
    , agg AS (
        SELECT
            uid
            , league_id
            , COUNT(*)                                              AS games
            , SUM(CASE WHEN team_points_for > team_points_against
                       THEN 1 ELSE 0 END)                          AS wins
            , SUM(CASE WHEN team_points_for = team_points_against
                       THEN 1 ELSE 0 END)                          AS draws
            , SUM(CASE WHEN team_points_for < team_points_against
                       THEN 1 ELSE 0 END)                          AS losses
            , SUM(team_points_for)                                  AS total_points_for
            , SUM(team_points_against)                              AS total_points_against
            , SUM(CASE
                WHEN team_points_for > team_points_against
                    THEN team_points_for
                WHEN team_points_for < team_points_against
                    THEN team_points_for * 0.5
                ELSE team_points_for
              END)                                                  AS winning_points_for
            , SUM(CASE
                WHEN team_points_for > team_points_against
                    THEN team_points_against
                WHEN team_points_for < team_points_against
                    THEN team_points_against * 1.5
                ELSE team_points_against
              END)                                                  AS winning_points_against
            , STRING_AGG(result, '' ORDER BY event_date DESC)       AS results
        FROM last_10
        WHERE rn <= 10
        GROUP BY uid, league_id
    )
    SELECT
        uid
        , league_id
        , games
        , wins
        , draws
        , losses
        , (wins + 0.5 * draws) / games                             AS win_percentage
        , total_points_for
        , total_points_against
        , winning_points_for
        , winning_points_against
        , results
    FROM agg;

CREATE INDEX idx_asset_last_10
    ON derived.asset_last_10 (uid);
