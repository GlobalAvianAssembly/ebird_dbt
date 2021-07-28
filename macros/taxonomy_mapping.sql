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
    species_with_same_binomial_as_ebird AS (
        SELECT
            scientific_name,
            taxonomy.common_name,
            scientific_name AS ebird_scientific_name,
            ebird.common_name AS ebird_common_name
        FROM {{ taxonomy }} taxonomy
            JOIN {{ ref('base_ebird_clements_taxonomy') }} ebird USING (scientific_name)
    ), species_manually_mapped AS (
        SELECT
            taxonomy.scientific_name AS scientific_name,
            taxonomy.common_name AS common_name,
            manual_mapping.ebird_scientific_name AS ebird_scientific_name,
            manual_mapping.ebird_common_name AS ebird_common_name
        FROM {{ taxonomy }} taxonomy
            JOIN {{ source('dropbox', 'avibase_manually_mapped_taxonomy') }} manual_mapping
                ON taxonomy.scientific_name = manual_mapping.alternative_scientific_name
        WHERE taxonomy.scientific_name NOT IN (SELECT scientific_name FROM {{ ref('base_ebird_clements_taxonomy') }})
    ), unmapped_species AS (
        SELECT
            scientific_name,
            common_name,
            CAST(NULL AS STRING) AS ebird_scientific_name,
            CAST(NULL AS STRING) AS ebird_common_name
        FROM {{ taxonomy }}
        WHERE scientific_name NOT IN (SELECT scientific_name FROM species_with_same_binomial_as_ebird)
            AND scientific_name NOT IN (SELECT scientific_name FROM species_manually_mapped)
    ), mapped_taxonomy AS (
        SELECT * FROM species_with_same_binomial_as_ebird
        UNION ALL
        SELECT * FROM species_manually_mapped
        UNION ALL
        SELECT * FROM unmapped_species
    )
    SELECT * FROM mapped_taxonomy ORDER BY scientific_name
{% endmacro %}