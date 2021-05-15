WITH distinct_ebird_species AS (
    SELECT DISTINCT
     scientific_name,
     common_name
    FROM {{ ref('int_species_at_hotspot') }}
), species_with_same_binomial_as_birdlife AS (
    SELECT
        scientific_name,
        distinct_ebird_species.common_name,
        scientific_name AS birdlife_scientific_name,
        birdlife.common_name AS birdlife_common_name
    FROM distinct_ebird_species
        JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife USING (scientific_name)
), species_with_alternative_binomial_in_birdlife AS (
    SELECT
        ebird.scientific_name,
        ebird.common_name,
        birdlife.scientific_name AS birdlife_scientific_name,
        birdlife.common_name AS birdlife_common_name
    FROM {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife
        JOIN UNNEST(birdlife.alternative_scientific_names) alternative_scientific_name
        JOIN distinct_ebird_species ebird ON ebird.scientific_name = alternative_scientific_name
), species_not_in_birdlife AS (
    SELECT
        distinct_ebird_species.scientific_name AS scientific_name,
        distinct_ebird_species.common_name AS common_name,
        manual_mapping.birdlife_scientific_name AS birdlife_scientific_name,
        birdlife.common_name AS birdlife_common_name
    FROM distinct_ebird_species
        JOIN {{ source('dropbox', 'avibase_mapped_ebird_taxonomy_missing_from_birdlife') }} manual_mapping
            ON distinct_ebird_species.scientific_name = manual_mapping.scientific_name
        JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife
            ON birdlife.scientific_name = manual_mapping.birdlife_scientific_name
), unmapped_species AS (
    SELECT
        scientific_name,
        common_name,
        CAST(NULL AS STRING) AS birdlife_scientific_name,
        CAST(NULL AS STRING) AS birdlife_common_name
    FROM distinct_ebird_species
    WHERE scientific_name NOT IN (SELECT scientific_name FROM species_with_same_binomial_as_birdlife)
        AND scientific_name NOT IN (SELECT scientific_name FROM species_with_alternative_binomial_in_birdlife)
        AND scientific_name NOT IN (SELECT scientific_name FROM species_not_in_birdlife)
), used_ebird_species AS (
    SELECT * FROM species_with_same_binomial_as_birdlife
    UNION ALL
    SELECT * FROM species_with_alternative_binomial_in_birdlife
    UNION ALL
    SELECT * FROM species_not_in_birdlife
    UNION ALL
    SELECT * FROM unmapped_species
)
SELECT * FROM used_ebird_species ORDER BY scientific_name

