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
), buffer_hotspots AS (
    SELECT
        city_id,
        COUNT(*) AS total_regional_hotspots,
        MAX(elevation) AS max_regional_hotspot_elevation,
        MIN(elevation) AS min_regional_hotspot_elevation
    FROM {{ ref('eph_included_hotspot') }}
    WHERE {{ is_regional() }}
    GROUP BY city_id
), urban_species AS (
    SELECT
        city_id,
        common_name,
        COUNT(*) AS number_of_hotspot_appearances
    FROM {{ ref('eph_included_species_at_hotspot') }}
    WHERE locality_id IN (SELECT locality_id FROM {{ ref('eph_included_hotspot') }} WHERE {{ is_urban() }})
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
        COUNT(DISTINCT common_name) AS regional_richness,
        COUNT(IF(number_of_hotspot_appearances = 1, 1, NULL)) AS one_observation,
        COUNT(IF(number_of_hotspot_appearances = 2, 1, NULL)) AS two_observations
    FROM {{ ref('regional_species') }}
    GROUP BY city_id
)
SELECT
    city.city_id,
    city.name,
    city.population_2015,
    city.built_area_2015,
    city.latitude,
    city.longitude,
    urban_hotspots.total_urban_hotspots AS total_urban_hotspots,
    buffer_hotspots.total_regional_hotspots AS total_regional_hotspots,
    urban_richness.urban_richness AS urban_richness,
    CASE
        WHEN urban_richness.two_observations > 0
        THEN ROUND(urban_richness.urban_richness + ((urban_richness.one_observation * urban_richness.one_observation) / (2 * urban_richness.two_observations)), 1)
        ELSE -1
    END AS urban_chao_estimate,
    regional_richness.regional_richness AS regional_richness,
    CASE
        WHEN regional_richness.two_observations > 0
        THEN ROUND(regional_richness.regional_richness + ((regional_richness.one_observation * regional_richness.one_observation) / (2 * regional_richness.two_observations)), 1)
        ELSE -1
    END AS regional_chao_estimate,
    urban_hotspots.min_urban_hotspot_elevation AS min_urban_hotspot_elevation,
    urban_hotspots.max_urban_hotspot_elevation AS max_urban_hotspot_elevation,
    buffer_hotspots.min_regional_hotspot_elevation AS min_regional_hotspot_elevation,
    buffer_hotspots.max_regional_hotspot_elevation AS max_regional_hotspot_elevation
FROM {{ ref ('city') }} city
JOIN urban_hotspots USING (city_id)
JOIN buffer_hotspots USING (city_id)
JOIN urban_richness USING (city_id)
JOIN regional_richness USING (city_id)