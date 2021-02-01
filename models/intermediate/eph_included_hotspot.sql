{{ config(materialized='ephemeral') }}

WITH checklist_counts AS (
    SELECT
        locality_id,
        COUNT(*) AS number_of_checklists
    FROM {{ ref('int_checklist') }}
    GROUP BY locality_id
)
SELECT
    hotspot_id,
    locality_id,
    name,
    latitude,
    longitude,
    city_id,
    elevation,
    type,
    number_of_checklists
FROM {{ ref('int_hotspot') }}
JOIN checklist_counts USING (locality_id)
WHERE {{ has_required_number_of_checklists('number_of_checklists') }}
