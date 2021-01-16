SELECT
    species.city_id AS city_id,
    common_name,
    scientific_name,
    ROUND(AVG(percentage_of_checklists), 2) AS average_percentage_of_checklists,
    COUNT(*) AS number_of_hotspot_appearances
FROM {{ ref('eph_included_species_at_hotspot') }} species
JOIN {{ ref('eph_included_hotspot') }} USING(locality_id)
WHERE {{ is_regional() }}
GROUP BY species.city_id, species.common_name, species.scientific_name