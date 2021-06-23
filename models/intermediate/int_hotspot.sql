SELECT
    {{ dbt_utils.surrogate_key(['locality_id', 'city_id']) }} AS hotspot_id,
    hotspot.locality_id,
    hotspot.name,
    hotspot.latitude,
    hotspot.longitude,
    hotspot_to_city.city_id,
    hotspot_to_city.elevation,
    hotspot_to_city.type
FROM {{ ref ('hotspot_to_city') }} hotspot_to_city
JOIN {{ ref('hotspot') }} hotspot USING (locality_id)
JOIN {{ ref('city') }} city USING (city_id)
WHERE {{ elevation_is_within_bounds('elevation', 'urban_hotspot_elevations.min', 'urban_hotspot_elevations.max') }}