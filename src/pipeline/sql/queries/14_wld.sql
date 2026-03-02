DROP TABLE IF EXISTS derived.wld;

CREATE TABLE derived.wld AS
    SELECT
        sat.uid
        , sat.league_id
        , l10.wins                          AS last_10_wins
        , l10.draws                         AS last_10_draws
        , l10.losses                        AS last_10_losses
        , l10.win_percentage                AS last_10_win_percentage
        , l10.results                       AS last_10_results
        , prv.wins                          AS last_season_wins
        , prv.draws                         AS last_season_draws
        , prv.losses                        AS last_season_losses
        , prv.win_percentage                AS last_season_win_percentage
        , cur.wins                          AS this_season_wins
        , cur.draws                         AS this_season_draws
        , cur.losses                        AS this_season_losses
        , cur.win_percentage                AS this_season_win_percentage
        , std.points                        AS this_season_points
        , std.points_per_week               AS this_season_points_per_week
        , sat.total_wins                    AS all_time_wins
        , sat.total_draws                   AS all_time_draws
        , sat.total_losses                  AS all_time_losses
        , sat.win_percentage_all_time       AS all_time_win_percentage
        , COALESCE(cur.end_rank_league,
                   prv.end_rank_league)     AS league_rank
        , l10.winning_points_for            AS asset_attack
        , l10.winning_points_against        AS asset_defense
    FROM derived.assets_stats_at AS sat
    LEFT JOIN derived.team_stats_previous AS prv  USING (uid)
    LEFT JOIN derived.team_stats_current  AS cur  USING (uid)
    LEFT JOIN derived.asset_last_10       AS l10  USING (uid)
    LEFT JOIN derived.assets_season_to_date AS std USING (uid);

CREATE INDEX idx_wld
    ON derived.wld (uid);
