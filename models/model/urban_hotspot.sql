{{ config(materialized='table') }}
WITH
checklist_distance_stats AS (
    SELECT
        locality_id,
        AVG(effort_distance_km) AS mean_effort_distance_km,
        MAX(effort_distance_km) AS max_effort_distance_km
    FROM {{ ref('int_checklist') }}
    WHERE effort_distance_km IS NOT NULL
    GROUP BY locality_id
),
checklist_area_stats AS (
    SELECT
        locality_id,
        AVG(effort_area_ha) AS mean_effort_area_ha,
        MAX(effort_area_ha) AS max_effort_area_ha
    FROM {{ ref('int_checklist') }}
    WHERE effort_area_ha IS NOT NULL
    GROUP BY locality_id
),
hotspot_data AS (
    SELECT
        hotspot_id,
        locality_id,
        name,
        latitude,
        longitude,
        city_id,
        elevation,
        number_of_checklists
    FROM {{ ref('eph_included_hotspot') }} hs
    WHERE {{ is_urban() }}
)
SELECT
    hotspot_data.*,
    STRUCT(
        checklist_distance_stats.mean_effort_distance_km,
        checklist_distance_stats.max_effort_distance_km,
        checklist_area_stats.mean_effort_area_ha,
        checklist_area_stats.max_effort_area_ha
    ) AS effort_summary,
    {{ landcover_struct('b500') }} AS percentage_landcover_500m,
    {{ landcover_struct('b1km') }} AS percentage_landcover_1km,
    {{ landcover_struct('b2km') }} AS percentage_landcover_2km,
    {{ landcover_struct('b3km') }} AS percentage_landcover_3km,
    {{ landcover_struct('b4km') }} AS percentage_landcover_4km,
    {{ landcover_struct('b5km') }} AS percentage_landcover_5km,
    row_number() OVER() AS row_number
FROM hotspot_data
LEFT JOIN checklist_distance_stats USING (locality_id)
LEFT JOIN checklist_area_stats USING (locality_id)
JOIN {{ source('dropbox', 'ee_hotspot_copernicus_land_coverage_and_pop_density') }} landcover ON landcover.hotspot_id = hotspot_data.locality_id
