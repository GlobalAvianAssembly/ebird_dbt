WITH alternative_names AS (
    SELECT
        scientific_name,
        SPLIT(alternative_scientific_names, ';') AS alternative_scientific_names
    FROM {{ ref('int_birdlife_taxonomy') }}
    WHERE alternative_scientific_names IS NOT NULL
), alternative_names_cleaned AS (
    SELECT
        scientific_name,
        TRIM(alternative_scientific_name) AS alternative_scientific_name
    FROM alternative_names
    JOIN UNNEST(alternative_scientific_names) alternative_scientific_name
    WHERE scientific_name != alternative_scientific_name
)
SELECT
    birdlife.scientific_name,
    birdlife.common_name,
    ARRAY(SELECT alternative_scientific_name FROM alternative_names_cleaned t WHERE t.scientific_name = birdlife.scientific_name) AS alternative_scientific_names,
FROM {{ ref('int_birdlife_taxonomy') }} birdlife
WHERE birdlife.scientific_name NOT IN (SELECT alternative_scientific_name FROM alternative_names_cleaned)