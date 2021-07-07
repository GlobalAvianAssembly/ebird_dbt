{{ config(materialized='table') }}
WITH distinct_merlin_species AS (
    SELECT DISTINCT
     scientific_name,
     common_name
    FROM {{ ref('base_merlin_species_pool') }}
),
{{ map_taxonomy('distinct_merlin_species') }}
