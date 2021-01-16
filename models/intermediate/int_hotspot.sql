{% set metres_above_max_elevation = 500 %}
{% set metres_below_min_elevation = 500 %}

SELECT
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
WHERE elevation < (max_urban_hotspot_elevation + {{ metres_above_max_elevation }})
AND elevation > (min_urban_hotspot_elevation - {{ metres_below_min_elevation }})