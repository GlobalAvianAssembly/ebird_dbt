{{ config(materialized='table') }}
WITH distinct_ebird_species AS (
    SELECT DISTINCT
     scientific_name,
     common_name
    FROM {{ ref('int_species_at_hotspot') }}
),
{{ map_taxonomy('distinct_ebird_species') }}
