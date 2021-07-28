
SELECT
    hotspot_id,
    species.locality_id AS locality_id,
    species.city_id AS city_id,
    species.common_name,
    species.scientific_name,
    ebird_scientific_name,
    ebird_common_name,
    percentage_of_checklists
FROM {{ ref('eph_included_species_at_hotspot') }} species
JOIN {{ ref('eph_included_hotspot') }} USING (hotspot_id)
JOIN {{ ref('used_ebird_taxonomy') }}  USING (scientific_name)
WHERE {{ is_urban() }}