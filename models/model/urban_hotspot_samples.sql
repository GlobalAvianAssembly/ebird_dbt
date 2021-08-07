WITH sample_ids AS (
    SELECT sample_id
    FROM  UNNEST(GENERATE_ARRAY(1, {{ var('number_of_times_to_sample') }})) sample_id
)
SELECT
    locality_id,
    sample_id
FROM
    {{ ref('urban_hotspot') }},
    sample_ids
