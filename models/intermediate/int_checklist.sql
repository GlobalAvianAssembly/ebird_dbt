SELECT
    checklist_id,
    locality_id,
    observation_date,
    effort_distance_km,
    effort_area_ha,
    duration_minutes
FROM {{ ref('checklist') }}
WHERE {{ effort_distance_is_within_bounds('effort_distance_km') }}
AND {{ duration_minutes_is_within_bounds('duration_minutes') }}
AND {{ year_is_included('observation_date') }}
AND locality_id IN (SELECT locality_id FROM {{ ref('int_hotspot') }})
