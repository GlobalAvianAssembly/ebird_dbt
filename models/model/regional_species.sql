{{ config(materialized='table') }}
WITH
ebird_city AS (
    SELECT DISTINCT
        city_id,
        scientific_name AS scientific_name,
        common_name AS common_name
    FROM
        {{ ref('urban_species') }}
),
birdlife AS (
    SELECT DISTINCT
        ebird_scientific_name AS scientific_name,
        ebird_common_name AS common_name,
        city_id
    FROM {{ ref('int_birdlife_regional_species_pool') }}
    JOIN {{ ref('used_birdlife_taxonomy') }} taxon_mapping USING (scientific_name)
),
merlin AS (
    SELECT DISTINCT
        merlin.scientific_name AS scientific_name,
        merlin.common_name AS common_name,
        city_id
    FROM {{ ref('int_merlin_regional_species_pool') }} merlin
    WHERE {{ merlin_pool_requirements('number_of_non_zero_frequency', 'longest_run_of_non_zero_frequency', 'smallest_precision') }}
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
    species_pools.present_in_city AS present_in_city
FROM species_pools
LEFT JOIN merlin
    ON species_pools.scientific_name = merlin.scientific_name AND species_pools.city_id = merlin.city_id
LEFT JOIN birdlife
    ON species_pools.scientific_name = birdlife.scientific_name AND species_pools.city_id = birdlife.city_id
LEFT JOIN ebird_city
    ON species_pools.scientific_name = ebird_city.scientific_name AND species_pools.city_id = ebird_city.city_id
JOIN {{ ref('city') }} city
    ON species_pools.city_id = city.city_id
WHERE species_pools.city_id IN (SELECT DISTINCT city_id FROM ebird_city)
ORDER BY present_in_city DESC, present_in_merlin DESC, present_in_birdlife DESC, species_pools.scientific_name