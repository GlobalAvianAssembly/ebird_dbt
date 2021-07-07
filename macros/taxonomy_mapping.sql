--------------------------------------------------------
-- Macro that maps a taxonomy to birdlife
--------------------------------------------------------

--
-- The taxonomy input is a table/view/ephemeral that looks like
-- SELECT DISTINCT
--     scientific_name,
--     common_name
-- FROM source
--
{% macro map_taxonomy(taxonomy) %}
    species_with_same_binomial_as_birdlife AS (
        SELECT
            scientific_name,
            taxonomy.common_name,
            scientific_name AS birdlife_scientific_name,
            birdlife.common_name AS birdlife_common_name
        FROM {{ taxonomy }} taxonomy
            JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife USING (scientific_name)
    ), species_with_alternative_binomial_in_birdlife AS (
        SELECT
            taxonomy.scientific_name,
            taxonomy.common_name,
            birdlife.scientific_name AS birdlife_scientific_name,
            birdlife.common_name AS birdlife_common_name
        FROM {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife
            JOIN UNNEST(birdlife.alternative_scientific_names) alternative_scientific_name
            JOIN {{ taxonomy }} taxonomy ON taxonomy.scientific_name = alternative_scientific_name
    ), species_not_in_birdlife AS (
        SELECT
            taxonomy.scientific_name AS scientific_name,
            taxonomy.common_name AS common_name,
            manual_mapping.birdlife_scientific_name AS birdlife_scientific_name,
            birdlife.common_name AS birdlife_common_name
        FROM {{ taxonomy }} taxonomy
            JOIN {{ source('dropbox', 'avibase_mapped_taxonomy_missing_from_birdlife') }} manual_mapping
                ON taxonomy.scientific_name = manual_mapping.scientific_name
            JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife
                ON birdlife.scientific_name = manual_mapping.birdlife_scientific_name
    ), unmapped_species AS (
        SELECT
            scientific_name,
            common_name,
            CAST(NULL AS STRING) AS birdlife_scientific_name,
            CAST(NULL AS STRING) AS birdlife_common_name
        FROM {{ taxonomy }}
        WHERE scientific_name NOT IN (SELECT scientific_name FROM species_with_same_binomial_as_birdlife)
            AND scientific_name NOT IN (SELECT scientific_name FROM species_with_alternative_binomial_in_birdlife)
            AND scientific_name NOT IN (SELECT scientific_name FROM species_not_in_birdlife)
    ), mapped_taxonomy AS (
        SELECT * FROM species_with_same_binomial_as_birdlife
        UNION ALL
        SELECT * FROM species_with_alternative_binomial_in_birdlife
        UNION ALL
        SELECT * FROM species_not_in_birdlife
        UNION ALL
        SELECT * FROM unmapped_species
    )
    SELECT * FROM mapped_taxonomy ORDER BY scientific_name
{% endmacro %}