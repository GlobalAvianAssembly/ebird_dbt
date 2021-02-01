WITH
city_data AS (
    SELECT
        city_id,
        city.name AS city_name,
        data.* EXCEPT (system_index, geo, city_name)
    FROM
        {{ source ('dropbox', 'ee_city_copernicus_land_coverage') }} data
    JOIN
        {{ ref('city') }} city ON city.name = data.city_name
),
hotspot_data AS (
    SELECT
        hotspot_id AS locality_id,
        * EXCEPT (system_index, geo, city_name, hotspot_id)
    FROM
        {{ source('dropbox', 'ee_hotspot_copernicus_land_coverage_and_pop_density') }}
)
SELECT
    hotspot_id,
    locality_id,
    hotspot.name AS locality_name,
    hotspot.richness AS locality_richness,
    hotspot.number_of_checklists,
    city_data.*,
    hotspot_data.* EXCEPT (locality_id)
FROM
    {{ ref('urban_hotspot') }} hotspot
JOIN
    hotspot_data USING (locality_id)
JOIN
    city_data USING (city_id)
