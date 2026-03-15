DROP TABLE IF EXISTS derived.current_elo CASCADE;

CREATE TABLE derived.current_elo AS
    SELECT
        COALESCE(cur.uid,              prv.uid)              AS uid
        , COALESCE(cur.league_id,      prv.league_id)        AS league_id
        , COALESCE(cur.end_of_season_elo,
                   prv.end_of_season_elo)                    AS current_elo
        , COALESCE(cur.last_elo_delta,
                   prv.last_elo_delta)                       AS elo_delta
        , COALESCE(cur.season_elo_delta,
                   prv.season_elo_delta)                     AS season_elo_delta
        , f.avg_win_probability_next_20_games                AS forecast
        , t.tier                                             AS tier
    FROM derived.team_stats_current AS cur
    FULL OUTER JOIN derived.team_stats_previous AS prv
        USING (uid)
    LEFT JOIN derived.forecast AS f
        ON f.uid = COALESCE(cur.uid, prv.uid)
    LEFT JOIN stats.team_tiers AS t
        ON t.uid = COALESCE(cur.uid, prv.uid)
    ORDER BY current_elo DESC NULLS LAST;

CREATE INDEX idx_current_elo
    ON derived.current_elo (uid);
