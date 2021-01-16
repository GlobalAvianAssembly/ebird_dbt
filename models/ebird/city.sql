WITH cities_from_urban_hotspots AS (
    SELECT
        row_number() OVER () AS city_id,
        CITY_NAME AS name,
        ROUND(CAST(POP_2015 AS NUMERIC), 0) AS population_2015,
        ROUND(CAST(BU_2015 AS NUMERIC), 0) AS built_area_2015,
        MIN(ELEVATION) AS min_urban_hotspot_elevation,
        MAX(ELEVATION) max_urban_hotspot_elevation
    FROM {{ source('dropbox', 'urban_hotspots') }}
    GROUP BY CITY_NAME, POP_2015, BU_2015
), nordpil_cities AS (
    SELECT
        City AS name,
        Latitude AS latitude,
        Longitude AS longitude,
    FROM {{ source('dropbox', 'nordpil_urbanareas1_1')}}
    WHERE City IN (SELECT CITY_NAME FROM {{ source('dropbox', 'urban_hotspots') }})
), cities_from_buffer_hotspots AS (
    SELECT
        CITY_NAME AS name,
        MIN(ELEVATION) AS min_regional_hotspot_elevation,
        MAX(ELEVATION) max_regional_hotspot_elevation
    FROM {{ source('dropbox', 'urban_100km_buffer_hotspots') }}
    GROUP BY CITY_NAME
)
SELECT
    city_id,
    name,
    population_2015,
    built_area_2015,
    latitude,
    longitude,
    min_urban_hotspot_elevation,
    max_urban_hotspot_elevation,
    min_regional_hotspot_elevation,
    max_regional_hotspot_elevation
FROM cities_from_urban_hotspots
JOIN nordpil_cities USING (name)
JOIN cities_from_buffer_hotspots USING (name)