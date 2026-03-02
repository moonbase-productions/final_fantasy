DROP TABLE IF EXISTS derived.assets_stats_at;

CREATE TABLE derived.assets_stats_at AS
    SELECT
        ts.uid
        , ts.league_id
        , SUM(ts.wins)                                              AS total_wins
        , SUM(ts.losses)                                            AS total_losses
        , SUM(ts.draws)                                             AS total_draws
        , CASE
            WHEN SUM(ts.wins) + SUM(ts.losses) + SUM(ts.draws) = 0
                THEN 0
            ELSE ROUND(
                100.0
                * (SUM(ts.wins) + 0.5 * SUM(ts.draws))
                / (SUM(ts.wins) + SUM(ts.losses) + SUM(ts.draws)),
                1
            )
          END                                                       AS win_percentage_all_time
    FROM stats.team_stats AS ts
    GROUP BY ts.uid, ts.league_id;

CREATE INDEX idx_assets_stats_at
    ON derived.assets_stats_at (uid);
