DROP TABLE IF EXISTS derived.leagues_current CASCADE;

CREATE TABLE derived.leagues_current AS
    SELECT
        al.league_id
        , al.league_name
        , al.league_name                AS league_name_clean
        , al.league_sport
        , lr.sport_type
        , lr.display_name
    FROM api.leagues AS al
    INNER JOIN admin.league_registry AS lr
        ON lr.league_id = al.league_id
    WHERE lr.is_active = TRUE;

CREATE INDEX idx_leagues_current
    ON derived.leagues_current (league_id);
