{{ config(materialized='table') }}

WITH urban_hotspots AS (
    SELECT
        city_id,
        COUNT(*) AS total_urban_hotspots,
        MAX(elevation) AS max_urban_hotspot_elevation,
        MIN(elevation) AS min_urban_hotspot_elevation
    FROM {{ ref('eph_included_hotspot') }}
    WHERE {{ is_urban() }}
    GROUP BY city_id
), urban_species AS (
    SELECT
        city_id,
        common_name,
        COUNT(*) AS number_of_hotspot_appearances
    FROM {{ ref('urban_species_birdlife') }}
    WHERE present_in_regional_pool = TRUE
    GROUP BY city_id, common_name
), urban_richness AS (
    SELECT
        city_id,
        COUNT(DISTINCT common_name) AS urban_richness,
        COUNT(IF(number_of_hotspot_appearances = 1, 1, NULL)) AS one_observation,
        COUNT(IF(number_of_hotspot_appearances = 2, 1, NULL)) AS two_observations
    FROM urban_species
    GROUP BY city_id
), regional_richness AS (
    SELECT
        city_id,
        COUNT(DISTINCT scientific_name) AS species_in_regional_pool
    FROM {{ ref('regional_species') }}
    WHERE present_in_birdlife_pool = TRUE
    GROUP BY city_id
), city_landcover AS (
    SELECT
        city_id,
        STRUCT(
            city_pc_bare AS bare,
            STRUCT(
                city_pc_closed_forest_deciduous_broadleaf AS deciduous_broadleaf,
                city_pc_closed_forest_deciduous_needle AS deciduous_needle,
                city_pc_closed_forest_evergreen_broadleaf AS evergreen_broadleaf,
                city_pc_closed_forest_evergreen_needle AS evergeen_needle,
                city_pc_closed_forest_forest_mixed AS mixed,
                city_pc_closed_forest_forest_other AS other
            ) AS closed_forest_cover,
            city_pc_closed_forest_forest_total AS closed_forest_total,
            city_pc_cultivated AS cultivated,
            city_pc_herbaceous_vegetation AS herbaceous_vegetation,
            city_pc_herbaceous_wetland AS herbaceous_wetland,
            city_pc_moss_and_lichen AS moss_and_lichen,
            city_pc_ocean AS ocean,
            STRUCT(
                city_pc_open_forest_deciduous_broadleaf AS deciduous_broadleaf,
                city_pc_open_forest_deciduous_needle AS deciduous_needle,
                city_pc_open_forest_evergreen_broadleaf AS evergreen_broadleaf,
                city_pc_open_forest_evergreen_needle AS evergeen_needle,
                city_pc_open_forest_forest_mixed AS mixed,
                city_pc_open_forest_forest_other AS other
            ) AS open_forest_cover,
            city_pc_open_forest_forest_total AS open_forest_total,
            city_pc_permanent_water AS permanent_water,
            city_pc_shrubs AS shrubs,
            city_pc_snow AS snow,
            city_pc_unknown AS unknown,
            city_pc_urban AS urban
        ) AS percentage_landcover,
        pop_2015 AS population_in_2015,
        city_calcuated_area AS total_area,
        STRUCT(
            max_elevation AS max,
            min_elevation AS min,
            max_elevation - min_elevation AS delta
        ) AS elevation,
        distance_to_coast
    FROM
        {{ source ('dropbox', 'ee_city_copernicus_land_coverage') }} data
    JOIN
        {{ source ('dropbox', 'ee_city_elevation_delta') }} elevation_data USING (city_name)
    JOIN
        {{ source ('dropbox', 'ee_city_distance_to_coastline') }} coast_distance USING (city_name)
    JOIN
        {{ ref('city') }} city ON city.name = data.city_name
),
merlin_data AS (
    SELECT
        city_id,
        precision.max AS max_precision,
        invalid_periods
    FROM {{ ref('base_merlin_effort') }}
    JOIN {{ ref('city') }} ON name = city_name
)
SELECT
    city.city_id,
    city.name,
    STRUCT(
        city.latitude,
        city.longitude
    ) AS location,
    regional_richness.species_in_regional_pool AS species_in_regional_pool,
    urban_richness.urban_richness AS urban_richness,
    CASE
        WHEN urban_richness.two_observations > 0
        THEN ROUND(urban_richness.urban_richness + ((urban_richness.one_observation * urban_richness.one_observation) / (2 * urban_richness.two_observations)), 1)
        ELSE -1
    END AS urban_chao_estimate,
    ROUND(urban_richness.urban_richness / regional_richness.species_in_regional_pool * 100, 1) AS percentage_of_regional_richness,
    STRUCT(
        urban_hotspots.min_urban_hotspot_elevation AS min_elevation,
        urban_hotspots.max_urban_hotspot_elevation AS max_elevation,
        urban_hotspots.total_urban_hotspots AS count
    ) AS urban_hotspots,
    STRUCT(
        max_precision,
        invalid_periods,
        periods_with_large_precision,
        total_unusable_periods
    ) AS merlin_quality,
    city_landcover.* EXCEPT (city_id)
FROM {{ ref ('city') }} city
JOIN urban_hotspots USING (city_id)
JOIN urban_richness USING (city_id)
JOIN regional_richness USING (city_id)
JOIN city_landcover USING(city_id)
JOIN merlin_data USING (city_id)