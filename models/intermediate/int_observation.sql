WITH observations AS (
    SELECT
        observation_id,
        scientific_name,
        common_name,
        CASE observation_count
            WHEN NULL THEN NULL
            WHEN 'X' THEN NULL
            ELSE CAST(observation_count AS Int64)
        END AS observation_count,
        CASE observation_count
            WHEN NULL THEN false
            WHEN 'X' THEN false
            ELSE true
        END AS is_observation_count_provided,
        checklist_id
    FROM {{ ref('observation')}}
)
SELECT
    scientific_name,
    common_name,
    SUM(observation_count) AS observation_count,
    LOGICAL_AND(is_observation_count_provided) AS is_observation_count_provided,
    checklist_id
FROM observations
WHERE checklist_id IN (SELECT checklist_id FROM {{ ref('int_checklist') }})
GROUP BY scientific_name, common_name, checklist_id