DROP TABLE IF EXISTS derived.web_assets;

CREATE TABLE derived.web_assets AS
    SELECT
        aa.uid
        , aa.league_id
        , lc.league_name
        , lc.league_name                                            AS league_name_clean
        , lc.league_sport
        , SUBSTRING(aa.uid FROM '[^-]+$')                          AS asset_id
        , aa.team_name                                             AS asset_name
        , aa.team_country                                          AS asset_country
        , ce.tier                                                  AS asset_tier
        , aa.team_logo                                             AS asset_logo
        , ce.current_elo                                           AS asset_elo
        , ce.elo_delta                                             AS asset_elo_delta
        , ce.season_elo_delta                                      AS asset_season_elo_delta
        , wld.league_rank                                          AS asset_league_rank
        , ce.forecast                                              AS asset_forecast
        , RANK() OVER (
            ORDER BY ce.current_elo DESC NULLS LAST
          )                                                        AS asset_overall_rank
        , ROUND((
            CUME_DIST() OVER (
                PARTITION BY lc.league_id
                ORDER BY wld.asset_attack ASC
            )
          ) * 100)                                                 AS asset_atk
        , ROUND((
            CUME_DIST() OVER (
                PARTITION BY lc.league_id
                ORDER BY wld.asset_defense DESC NULLS LAST
            )
          ) * 100)                                                 AS asset_def
        , COALESCE(ps_luck.luck_display, 50)                       AS asset_luck
        , wld.last_10_wins
        , wld.last_10_draws
        , wld.last_10_losses
        , wld.last_10_win_percentage
        , wld.last_10_results
        , wld.last_season_wins
        , wld.last_season_draws
        , wld.last_season_losses
        , wld.last_season_win_percentage
        , wld.this_season_wins
        , wld.this_season_draws
        , wld.this_season_losses
        , wld.this_season_win_percentage
        , wld.this_season_points
        , wld.this_season_points_per_week
        , wld.all_time_wins
        , wld.all_time_draws
        , wld.all_time_losses
        , wld.all_time_win_percentage
        , CURRENT_TIMESTAMP                                        AS updated_at
    FROM api.assets AS aa
    INNER JOIN derived.leagues_current AS lc
        ON lc.league_id = aa.league_id
    LEFT JOIN derived.wld AS wld
        USING (uid)
    LEFT JOIN derived.current_elo AS ce
        USING (uid)
    LEFT JOIN (
        SELECT DISTINCT ON (uid)
            uid
            , luck_display
        FROM stats.team_stats
        ORDER BY uid, league_season DESC NULLS LAST
    ) AS ps_luck
        USING (uid);

CREATE INDEX idx_web_assets
    ON derived.web_assets (uid);
