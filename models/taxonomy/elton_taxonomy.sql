WITH elton_species AS (
    SELECT DISTINCT
     *
    FROM {{ ref('base_elton_taxonomy') }}
), mapped_taxonomy AS (
    WITH elton_taxonomy AS (
        SELECT DISTINCT scientific_name, common_name FROM elton_species
    ),
    {{ map_taxonomy('elton_taxonomy') }}
)
SELECT
    mapped.birdlife_scientific_name,
    mapped.birdlife_common_name,
    elton_species.*
FROM elton_species
LEFT JOIN mapped_taxonomy USING (scientific_name)