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
    FROM {{ ref('base_resolve_to_ecosystem') }}
),
biomes AS (
    SELECT
        city,
        biome_id,
        biome_name,
        SUM(area_of_city) AS area_of_city,
    FROM ecosystems
    GROUP BY city, biome_id, biome_name
),
realms AS (
    SELECT
        DISTINCT city, realm
    FROM ecosystems
),
recovery AS (
    SELECT
        city,
        recovery_id,
        recovery_description,
        SUM(area_of_city) AS area_of_city,
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
                area_of_city
            )
        FROM biomes
        WHERE cities.city = biomes.city
    ) AS biomes,
    SELECT
        STRUCT(
            biome_id,
            biome_name,
            area_of_city
        )
    FROM biomes WHERE cities.city = biomes.city ORDER BY area_of_city DESC LIMIT 1 AS biome,
    SELECT realm FROM realms WHERE cities.city = realms.city AS realm,
    ARRAY(
        SELECT
            STRUCT(
                recovery_id,
                recovery_description,
                area_of_city
            )
        FROM recovery
        WHERE cities.city = biomes.city
    ) AS recovery_areas,
    SELECT
        STRUCT(
            recovery_id,
            recovery_description
        )
    FROM recovery WHERE cities.city = recovery.city ORDER BY area_of_city DESC LIMIT 1 AS recovery,
FROM cities