{% set max_effort_distance_km = 10 %}
{% set min_duration_minutes = 5 %}
{% set max_duration_minutes = 240 %}
{% set earliest_year = 2014 %}

SELECT
    checklist_id,
    locality_id,
    observation_date,
    effort_distance_km,
    effort_area_ha,
    duration_minutes
FROM {{ ref('checklist') }}
WHERE (effort_distance_km IS NULL OR effort_distance_km <= {{max_effort_distance_km}})
AND duration_minutes >= {{min_duration_minutes}} AND duration_minutes <= {{max_duration_minutes}}
AND EXTRACT(YEAR FROM observation_date) >= {{earliest_year}}
AND locality_id IN (SELECT locality_id FROM {{ ref('int_hotspot') }})
