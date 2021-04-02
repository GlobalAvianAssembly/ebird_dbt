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
        city_id
    FROM {{ ref('int_birdlife_regional_species_pool') }}
),
all_species AS (
    SELECT DISTINCT
        scientific_name,
        city_id
    FROM (SELECT * FROM ebird UNION ALL SELECT * FROM birdlife)
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
    t.common_name AS common_name,
    species_pools.city_id AS city_id,
    species_pools.present_in_ebird AS present_in_ebird,
    species_pools.present_in_birdlife AS present_in_birdlife
FROM species_pools
LEFT JOIN {{ ref('int_birdlife_taxonomy') }} t
    ON species_pools.scientific_name = t.scientific_name
ORDER BY present_in_ebird DESC, present_in_birdlife DESC, species_pools.scientific_name

