WITH hotspot_richness AS (
    SELECT
        locality_id,
        COUNT(*) AS richness
    FROM {{ ref('urban_species' )}}
    GROUP BY locality_id
)
SELECT
    locality_id,
    name,
    latitude,
    longitude,
    city_id,
    elevation,
    number_of_checklists,
    richness
FROM {{ ref('eph_included_hotspot') }}
JOIN hotspot_richness USING(locality_id)
WHERE {{ is_urban() }}