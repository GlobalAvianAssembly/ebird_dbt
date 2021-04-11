WITH elton_species AS (
    SELECT DISTINCT
     *
    FROM {{ ref('base_elton_taxonomy') }}
), species_with_same_binomial_as_birdlife AS (
    SELECT
        elton_species.*,
        scientific_name AS birdlife_scientific_name,
        birdlife.common_name AS birdlife_common_name
    FROM elton_species
        JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife USING (scientific_name)
), species_with_alternative_binomial_in_birdlife AS (
    SELECT
        elton.*,
        birdlife.scientific_name AS birdlife_scientific_name,
        birdlife.common_name AS birdlife_common_name
    FROM {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife
        JOIN UNNEST(birdlife.alternative_scientific_names) alternative_scientific_name
        JOIN elton_species elton ON elton.scientific_name = alternative_scientific_name
), species_not_in_birdlife AS (
    SELECT
        elton_species.*,
        CAST(NULL AS STRING) AS birdlife_scientific_name,
        CAST(NULL AS STRING) AS birdlife_common_name
    FROM elton_species
    WHERE scientific_name NOT IN (SELECT scientific_name FROM species_with_same_binomial_as_birdlife)
        AND scientific_name NOT IN (SELECT scientific_name FROM species_with_alternative_binomial_in_birdlife)
), all_elton_species AS (
    SELECT * FROM species_with_same_binomial_as_birdlife
    UNION ALL
    SELECT * FROM species_with_alternative_binomial_in_birdlife
    UNION ALL
    SELECT * FROM species_not_in_birdlife
)
SELECT * FROM all_elton_species ORDER BY scientific_name