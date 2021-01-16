{% set required_percentage_of_checklists = 10 %}

{{ config(materialized='view') }}

SELECT *
FROM {{ ref('int_species_at_hotspot') }}
WHERE percentage_of_checklists >= {{ required_percentage_of_checklists }}