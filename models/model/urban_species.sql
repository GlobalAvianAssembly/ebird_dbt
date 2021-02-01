
SELECT
    hotspot_id,
    species.locality_id AS locality_id,
    species.city_id AS city_id,
    common_name,
    scientific_name,
    percentage_of_checklists
FROM {{ ref('eph_included_species_at_hotspot') }} species
JOIN {{ ref('eph_included_hotspot') }} USING (hotspot_id)
WHERE {{ is_urban() }}