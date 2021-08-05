{{ config(materialized='table') }}

WITH
urban_hotspots AS (
    SELECT
        city_id,
        COUNT(*) AS total_urban_hotspots,
        MAX(elevation) AS max_urban_hotspot_elevation,
        MIN(elevation) AS min_urban_hotspot_elevation
    FROM {{ ref('eph_included_hotspot') }}
    WHERE {{ is_urban() }}
    GROUP BY city_id
),
city_landcover AS (
    SELECT
        city_id,
        {{ landcover_struct('city') }} AS percentage_landcover_urban,
        {{ landcover_struct('region') }} AS percentage_landcover_region,
        city_calcuated_area AS total_area
    FROM
        {{ source ('dropbox', 'ee_city_copernicus_land_coverage') }} data
    JOIN
        {{ source ('dropbox', 'ee_city_elevation_delta') }} elevation_data USING (city_name)
    JOIN
        {{ ref('city') }} city ON city.name = data.city_name
),
merlin_data AS (
    SELECT
        city_id,
        precision.max AS max_precision,
        invalid_periods,
        total_unusable_periods
    FROM {{ ref('base_merlin_effort') }}
    JOIN {{ ref('city') }} ON name = city_name
)
SELECT
    city.*,
    STRUCT(
        urban_hotspots.min_urban_hotspot_elevation AS min_elevation,
        urban_hotspots.max_urban_hotspot_elevation AS max_elevation,
        urban_hotspots.total_urban_hotspots AS count
    ) AS urban_hotspots,
    STRUCT(
        max_precision,
        invalid_periods,
        total_unusable_periods
    ) AS merlin_quality,
    city_landcover.* EXCEPT(city_id)
FROM {{ ref ('city') }} city
JOIN urban_hotspots USING (city_id)
JOIN city_landcover USING(city_id)
JOIN merlin_data USING (city_id)