{{ config(materialized='ephemeral') }}
WITH
ecosystems AS (
    SELECT
        city,
        ecosystem_id,
        ecosystem_name,
        biome_id,
        biome_name,
        realm,
        recovery_description,
        recovery_id,
        area_of_city,
        ecosystem_biome_reference
    FROM {{ ref('base_resolve_city_to_ecosystem') }}
),
biomes AS (
    SELECT
        city,
        biome_id,
        biome_name,
        SUM(area_of_city) AS area_of_city,
        row_number() OVER (PARTITION BY city ORDER BY SUM(area_of_city) DESC) AS rank
    FROM ecosystems
    GROUP BY city, biome_id, biome_name
),
realms AS (
    SELECT
        city,
        realm,
        SUM(area_of_city) AS area_of_city,
         row_number() OVER (PARTITION BY city ORDER BY SUM(area_of_city) DESC) AS rank
    FROM ecosystems
    GROUP BY city, realm
),
recovery AS (
    SELECT
        city,
        recovery_id,
        recovery_description,
        SUM(area_of_city) AS area_of_city,
        row_number() OVER (PARTITION BY city ORDER BY SUM(area_of_city) DESC) AS rank
    FROM ecosystems
    GROUP BY city, recovery_id, recovery_description
),
cities AS (
    SELECT
        DISTINCT city
    FROM biomes
)
SELECT
    city AS name,
    ARRAY(
        SELECT
            STRUCT(
                ecosystem_id,
                ecosystem_name,
                biome_id,
                biome_name,
                realm,
                recovery_description,
                recovery_id,
                area_of_city,
                ecosystem_biome_reference
            )
        FROM ecosystems
        WHERE cities.city = ecosystems.city
    ) AS ecosystems,
    ARRAY(
        SELECT
            STRUCT(
                biome_id,
                biome_name,
                biomes.area_of_city
            )
        FROM biomes
        WHERE cities.city = biomes.city
    ) AS biomes,
    STRUCT(
        biome_id,
        biome_name
    ) AS biome,
    realm,
    ARRAY(
        SELECT
            STRUCT(
                recovery_id,
                recovery_description,
                recovery.area_of_city
            )
        FROM recovery
        WHERE cities.city = recovery.city
    ) AS recovery_areas,
    STRUCT(
        recovery_id,
        recovery_description
    ) AS recovery,
FROM cities
JOIN biomes top_biome USING (city)
JOIN recovery top_recovery USING (city)
JOIN realms USING (city)
WHERE
    top_biome.rank = 1
AND
    top_recovery.rank = 1
AND
    realms.rank = 1