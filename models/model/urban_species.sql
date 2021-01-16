{% set required_percentage_of_checklists = 10 %}

SELECT
    locality_id,
    species.city_id AS city_id,
    common_name,
    scientific_name,
    percentage_of_checklists
FROM {{ ref('int_included_species_at_hotspot') }} species
JOIN {{ ref('int_included_hotspot') }} USING (locality_id)
WHERE {{ is_urban() }}