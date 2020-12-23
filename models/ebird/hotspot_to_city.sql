WITH
urban_hotspots AS (
    SELECT
        LOCALITY_ID,
        CITY_NAME,
        ELEVATION,
        'urban' AS type
    FROM {{ ref("urban_hotspots") }}
),
buffer_hotspots AS  (
    SELECT
        LOCALITY_ID,
        CITY_NAME,
        ELEVATION,
        'buffer' AS type
    FROM {{ ref("urban_100km_buffer_hotspots") }}
),
all_hotspots AS (
    (SELECT * FROM urban_hotspots) UNION ALL (SELECT * FROM buffer_hotspots)
)
SELECT
    LOCALITY_ID AS locality_id,
    city_id,
    ELEVATION AS elevation,
    type
FROM all_hotspots
LEFT OUTER JOIN {{ ref("city") }} ON all_hotspots.CITY_NAME = city.name