SELECT
    global_unique_identifier AS observation_id,
    scientific_name,
    subspecies_scientific_name,
    common_name,
    subspecies_common_name,
    observation_count,
    sampling_event_identifier AS checklist_id
FROM {{ source('dropbox', 'ebird') }}
WHERE sampling_event_identifier IN
    (SELECT checklist_id FROM {{ ref('checklist') }})
    AND approved = 1
    AND category = 'species'