WITH hotspot_richness AS (
    SELECT
        {{ dbt_utils.surrogate_key(['locality_id', 'city_id']) }} AS hotspot_id,
        COUNT(*) AS richness
    FROM {{ ref('urban_species' )}}
    GROUP BY locality_id, city_id
)
SELECT
    hotspot_id,
    locality_id,
    name,
    latitude,
    longitude,
    city_id,
    elevation,
    number_of_checklists,
    richness
FROM {{ ref('eph_included_hotspot') }} hs
JOIN hotspot_richness hr USING (hotspot_id)
WHERE {{ is_urban() }}