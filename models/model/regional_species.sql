WITH
ebird AS (
    SELECT DISTINCT
        taxonomy_join.birdlife_scientific_name AS scientific_name,
        taxonomy_join.birdlife_common_name AS common_name,
        ebird.city_id,
        SUM(number_of_hotspot_appearances) AS number_of_hotspot_appearances
    FROM {{ ref('int_ebird_regional_species_pool') }} ebird
    JOIN {{ ref('used_ebird_taxonomy')}} taxonomy_join
        ON ebird.scientific_name = taxonomy_join.scientific_name
    GROUP BY
        taxonomy_join.birdlife_scientific_name,
        taxonomy_join.birdlife_common_name,
        ebird.city_id
),
birdlife AS (
    SELECT
        scientific_name,
        common_name,
        city_id,
        presence
    FROM {{ ref('int_birdlife_regional_species_pool') }}
),
all_species AS (
    SELECT DISTINCT
        *
    FROM (
        SELECT scientific_name, common_name, city_id FROM ebird
        UNION ALL
        SELECT scientific_name, common_name, city_id FROM birdlife
    )
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
    species_pools.common_name AS common_name,
    species_pools.city_id AS city_id,
    species_pools.present_in_ebird AS present_in_ebird,
    species_pools.present_in_birdlife AS present_in_birdlife,
    COALESCE(ebird.number_of_hotspot_appearances, 0) AS number_of_ebird_hotspot_appearances,
    birdlife.presence AS birdlife_presence,
FROM species_pools
LEFT JOIN ebird
    ON species_pools.scientific_name = ebird.scientific_name AND species_pools.city_id = ebird.city_id
LEFT JOIN birdlife
    ON species_pools.scientific_name = birdlife.scientific_name AND species_pools.city_id = birdlife.city_id
ORDER BY present_in_ebird DESC, present_in_birdlife DESC, species_pools.scientific_name

