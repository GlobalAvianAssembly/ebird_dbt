{{ config(materialized='table') }}
WITH
ebird_city AS (
    SELECT
        city_id,
        ebird_scientific_name AS scientific_name,
        ebird_common_name AS common_name,
        COUNT(*) AS number_of_hotspot_appearances
    FROM
        {{ ref('urban_species') }}
    GROUP BY
        ebird_scientific_name,
        ebird_common_name,
        city_id
),
birdlife_dedup AS (
    SELECT
        ebird_scientific_name AS scientific_name,
        ebird_common_name AS common_name,
        city_id,
        iucn_red_list_2020,
        presence,
        row_number() OVER (PARTITION BY ebird_scientific_name, ebird_common_name, city_id ORDER BY iucn_red_list_order ASC) AS rownum
    FROM {{ ref('int_birdlife_regional_species_pool') }}
    JOIN {{ ref('used_birdlife_taxonomy') }} USING(scientific_name)
),
birdlife AS (
    SELECT * EXCEPT(rownum)
    FROM birdlife_dedup
    WHERE rownum = 1
),
merlin AS (
    SELECT
        taxonomy.ebird_scientific_name AS scientific_name,
        taxonomy.ebird_common_name AS common_name,
        city_id,
        MAX(number_of_non_zero_frequency) AS merlin_number_of_non_zero_frequency,
        MAX(longest_run_of_non_zero_frequency) AS merlin_longest_run_of_non_zero_frequency,
        MIN(smallest_precision) AS merlin_smallest_precision
    FROM {{ ref('int_merlin_regional_species_pool') }} merlin
    JOIN {{ ref('merlin_taxonomy') }} taxonomy USING(scientific_name)
    GROUP BY
        taxonomy.ebird_scientific_name,
        taxonomy.ebird_common_name,
        city_id
),
all_species AS (
    SELECT DISTINCT
        *
    FROM (
        SELECT scientific_name, common_name, city_id FROM ebird_city
        UNION ALL
        SELECT scientific_name, common_name, city_id FROM birdlife
        UNION ALL
        SELECT scientific_name, common_name, city_id FROM merlin
    )
),
species_pools AS (
    SELECT
        all_species.* ,
        IF(EXISTS(
            SELECT merlin.scientific_name FROM merlin WHERE merlin.scientific_name = all_species.scientific_name AND merlin.city_id = all_species.city_id
        ), TRUE, FALSE) AS present_in_merlin,
        IF(EXISTS(
            SELECT birdlife.scientific_name FROM birdlife WHERE birdlife.scientific_name = all_species.scientific_name AND birdlife.city_id = all_species.city_id
        ), TRUE, FALSE) AS present_in_birdlife,
        IF(EXISTS(
            SELECT ebird_city.scientific_name FROM ebird_city WHERE ebird_city.scientific_name = all_species.scientific_name AND ebird_city.city_id = all_species.city_id
        ), TRUE, FALSE) AS present_in_city
    FROM all_species
)
SELECT
    species_pools.scientific_name AS scientific_name,
    species_pools.common_name AS common_name,
    city.name AS city_name,
    city.city_id AS city_id,
    species_pools.present_in_merlin AS present_in_merlin_pool,
    species_pools.present_in_birdlife AS present_in_birdlife_pool,
    (species_pools.present_in_merlin AND species_pools.present_in_birdlife) AS present_in_both_pools,
    (species_pools.present_in_merlin OR species_pools.present_in_birdlife) AS present_in_either_pool,
    species_pools.present_in_city AS present_in_city,
    COALESCE(ebird_city.number_of_hotspot_appearances, 0) AS number_of_urban_ebird_hotspot_appearances,
    merlin_number_of_non_zero_frequency,
    merlin_longest_run_of_non_zero_frequency,
    merlin_smallest_precision,
    birdlife.iucn_red_list_2020,
    birdlife.presence AS birdlife_presence,
    taxon.* EXCEPT(scientific_name, common_name)
FROM species_pools
LEFT JOIN merlin
    ON species_pools.scientific_name = merlin.scientific_name AND species_pools.city_id = merlin.city_id
LEFT JOIN birdlife
    ON species_pools.scientific_name = birdlife.scientific_name AND species_pools.city_id = birdlife.city_id
LEFT JOIN ebird_city
    ON species_pools.scientific_name = ebird_city.scientific_name AND species_pools.city_id = ebird_city.city_id
JOIN {{ ref('city') }} city
    ON species_pools.city_id = city.city_id
LEFT JOIN {{ ref('taxonomy') }} taxon
    ON species_pools.scientific_name = taxon.scientific_name
WHERE species_pools.city_id IN (SELECT DISTINCT city_id FROM ebird_city)
ORDER BY present_in_city DESC, present_in_merlin DESC, present_in_birdlife DESC, species_pools.scientific_name