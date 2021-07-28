{{ config(materialized='table') }}
WITH
distinct_used_birdlife_taxonomy AS (
    SELECT DISTINCT
        scientific_name,
        common_name
    FROM {{ ref('int_birdlife_regional_species_pool') }}
),
matching_bionomial AS (
    SELECT
        scientific_name,
        taxonomy.common_name,
        scientific_name AS ebird_scientific_name,
        ebird.common_name AS ebird_common_name
    FROM distinct_used_birdlife_taxonomy taxonomy
        JOIN {{ ref('base_ebird_clements_taxonomy') }} ebird USING (scientific_name)
),
alternative_bionomial AS (
    SELECT
        taxonomy.scientific_name,
        taxonomy.common_name,
        ebird.scientific_name AS ebird_scientific_name,
        ebird.common_name AS ebird_common_name
    FROM distinct_used_birdlife_taxonomy taxonomy
        JOIN {{ ref('birdlife_taxonomy_with_alternatives')}} USING (scientific_name)
        JOIN UNNEST(alternative_scientific_names) alternative
        JOIN {{ ref('base_ebird_clements_taxonomy') }} ebird ON ebird.scientific_name = alternative
    WHERE
        taxonomy.scientific_name NOT IN (SELECT scientific_name FROM matching_bionomial)
),
manually_mapped_bionomial AS (
    SELECT
        taxonomy.scientific_name AS scientific_name,
        taxonomy.common_name AS common_name,
        manual_mapping.ebird_scientific_name AS ebird_scientific_name,
        manual_mapping.ebird_common_name AS ebird_common_name
    FROM distinct_used_birdlife_taxonomy taxonomy
        JOIN {{ source('dropbox', 'avibase_manually_mapped_taxonomy') }} manual_mapping
            ON taxonomy.scientific_name = manual_mapping.alternative_scientific_name
    WHERE
        taxonomy.scientific_name NOT IN (SELECT scientific_name FROM matching_bionomial)
        AND
        taxonomy.scientific_name NOT IN (SELECT scientific_name FROM alternative_bionomial WHERE ebird_scientific_name IS NOT NULL)
),
missing_bionomial AS (
    SELECT
        scientific_name,
        common_name,
        CAST(NULL AS STRING) AS ebird_scientific_name,
        CAST(NULL AS STRING) AS ebird_common_name
    FROM distinct_used_birdlife_taxonomy
    WHERE
        scientific_name NOT IN (SELECT scientific_name FROM matching_bionomial WHERE ebird_scientific_name IS NOT NULL)
        AND
        scientific_name NOT IN (SELECT scientific_name FROM alternative_bionomial WHERE ebird_scientific_name IS NOT NULL)
        AND
        scientific_name NOT IN (SELECT scientific_name FROM manually_mapped_bionomial WHERE ebird_scientific_name IS NOT NULL)
),
taxonomy AS (
    SELECT * FROM matching_bionomial WHERE ebird_scientific_name IS NOT NULL
    UNION ALL
    SELECT * FROM alternative_bionomial WHERE ebird_scientific_name IS NOT NULL
    UNION ALL
    SELECT * FROM manually_mapped_bionomial
    UNION ALL
    SELECT * FROM missing_bionomial
)
SELECT * FROM taxonomy ORDER BY scientific_name