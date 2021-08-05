{{ config(materialized='table') }}
SELECT DISTINCT
    checklist_id,
    scientific_name,
    taxa.common_name
FROM {{ ref('int_observation') }}
JOIN {{ ref('taxonomy') }} taxa USING(scientific_name)
WHERE {{ filter_to_accepted_taxonomy() }}