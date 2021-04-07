WITH
ebird AS (
    SELECT
        scientific_name,
        city_id
    FROM {{ ref('int_ebird_regional_species_pool') }}
),
birdlife AS (
    SELECT
        scientific_name,
        city_id,
        seasonal,
        seasonal_code,
        presence,
        presence_code,
        origin,
        origin_code
    FROM {{ ref('int_birdlife_regional_species_pool') }}
),
all_species AS (
    SELECT DISTINCT
        scientific_name,
        city_id
    FROM (SELECT * FROM ebird UNION ALL SELECT scientific_name, city_id FROM birdlife)
),
species_pools AS (
    SELECT
        all_species.* ,
        IF(EXISTS(
            SELECT ebird.scientific_name FROM ebird WHERE ebird.scientific_name = all_species.scientific_name AND ebird.city_id = all_species.city_id
        ), TRUE, FALSE) AS present_in_ebird,
        IF(EXISTS(
            SELECT birdlife.scientific_name FROM birdlife WHERE birdlife.scientific_name = all_species.scientific_name AND birdlife.city_id = all_species.city_id
        ), TRUE, FALSE) AS present_in_birdlife
    FROM all_species
)
SELECT
    species_pools.scientific_name AS scientific_name,
    COALESCE(t.common_name, ebird.common_name) AS common_name,
    species_pools.city_id AS city_id,
    species_pools.present_in_ebird AS present_in_ebird,
    species_pools.present_in_birdlife AS present_in_birdlife,
    COALESCE(ebird.number_of_hotspot_appearances, 0) AS number_of_ebird_hotspot_appearances,
    birdlife.seasonal AS birdlife_seasonal,
    birdlife.presence AS birdlife_presence,
    birdlife.origin AS birdlife_origin,
    birdlife.seasonal_code AS birdlife_seasonal_code,
    birdlife.presence_code AS birdlife_presence_code,
    birdlife.origin_code AS birdlife_origin_code
FROM species_pools
LEFT JOIN {{ ref('int_birdlife_taxonomy') }} t
    ON species_pools.scientific_name = t.scientific_name
LEFT JOIN {{ ref('int_ebird_regional_species_pool') }} ebird
    ON species_pools.scientific_name = ebird.scientific_name AND species_pools.city_id = ebird.city_id
LEFT JOIN {{ ref('int_birdlife_regional_species_pool') }} birdlife
    ON species_pools.scientific_name = birdlife.scientific_name AND species_pools.city_id = birdlife.city_id
ORDER BY present_in_ebird DESC, present_in_birdlife DESC, species_pools.scientific_name

