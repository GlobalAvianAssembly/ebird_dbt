{{ config(materialized='table') }}
WITH distinct_merlin_species AS (
    SELECT DISTINCT
     scientific_name,
     common_name
    FROM {{ ref('base_merlin_species_pool') }}
),
mapped_taxonomy AS (
    WITH {{ map_taxonomy('distinct_merlin_species') }}
)
SELECT * FROM mapped_taxonomy WHERE ebird_scientific_name IS NOT NULL