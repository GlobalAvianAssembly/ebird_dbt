{{ config(materialized='table') }}
WITH used_taxonomy AS (
    SELECT ebird_scientific_name FROM {{ ref('used_merlin_taxonomy') }}
    UNION ALL
    SELECT ebird_scientific_name FROM {{ ref('used_birdlife_taxonomy') }}
    UNION ALL
    SELECT ebird_scientific_name FROM {{ ref('used_ebird_taxonomy') }}
),
distinct_taxonomy AS (
    SELECT DISTINCT
        ebird_scientific_name
    FROM used_taxonomy
)
SELECT
    ebird_scientific_name AS scientific_name,
    ebird.common_name AS common_name,
    ebird.t_order AS taxonomic_order,
    ebird.t_family AS taxonomic_family
FROM distinct_taxonomy
JOIN {{ ref('base_ebird_clements_taxonomy') }} ebird ON ebird_scientific_name = ebird.scientific_name