WITH
hotspots AS (
    SELECT
        locality_id,
        locality as name,
        latitude,
        longitude,
        county_code,
        state_code,
        country_code,
        row_number() OVER (PARTITION BY locality_id ORDER BY global_unique_identifier) AS locality_id_rownum
    FROM {{ source('dropbox', 'ebird') }}
    WHERE {{ is_required_hotspot_type() }}
)
SELECT
    * EXCEPT(locality_id_rownum)
FROM hotspots
WHERE locality_id_rownum = 1