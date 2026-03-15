DROP TABLE IF EXISTS derived.web_assets_info CASCADE;

CREATE TABLE derived.web_assets_info AS
    SELECT aa.*
    FROM api.assets AS aa
    WHERE aa.uid IN (
        SELECT uid FROM derived.web_assets
    );
