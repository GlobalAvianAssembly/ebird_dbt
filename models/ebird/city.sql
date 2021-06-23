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
)
SELECT
    city_id,
    name,
    population_2015,
    built_area_2015,
    country_code,
    STRUCT(
        latitude,
        longitude,
        country,
        continent,
        subregion
    ) AS location,
    STRUCT(
        min_urban_hotspot_elevation AS min,
        max_urban_hotspot_elevation AS max
    ) AS urban_hotspot_elevations,
    STRUCT(
        gdp_estimate,
        economy_description,
        income_group
    ) AS country_economy,
    STRUCT(
        ecosystems,
        biomes,
        biome,
        realm,
        recovery_areas,
        recovery
    ) AS ecosystem
FROM cities_from_urban_hotspots
JOIN {{ ref('base_city_nordpil_data') }} USING (name)
JOIN {{ ref('base_world_bank_city_to_country') }} USING (name)
JOIN {{ ref('biome') }} USING(name)