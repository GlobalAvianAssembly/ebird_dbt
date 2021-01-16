{{ config(materialized='ephemeral') }}

SELECT *
FROM {{ ref('int_species_at_hotspot') }}
WHERE {{ appears_on_required_percentage_of_checklists('percentage_of_checklists') }}