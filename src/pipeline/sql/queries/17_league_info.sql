DROP TABLE IF EXISTS derived.league_info CASCADE;

CREATE TABLE derived.league_info AS
    SELECT
        league_id
        , COUNT(DISTINCT uid)   AS league_asset_count
        , 0                     AS league_asset_min
        , 3                     AS league_asset_max
    FROM derived.web_assets
    GROUP BY league_id;
