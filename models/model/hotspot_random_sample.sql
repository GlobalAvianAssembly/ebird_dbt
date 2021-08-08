{{ config(materialized='incremental') }}

SELECT
    0 AS sample_id,
    '' AS scientific_name,
    '' AS common_name,
    0.0 AS percentage_of_checklists,
    '' AS locality_id
LIMIT 0