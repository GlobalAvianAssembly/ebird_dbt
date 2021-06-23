SELECT
    NAME_MAIN AS city,
    ECO_ID AS ecosystem_id,
    ECO_NAME AS ecosystem_name,
    BIOME_ID AS biome_id,
    BIOME_NAME AS biome_name,
    REALM AS realm,
    NNH_NAME AS recovery_description,
    NNH AS recovery_id,
    AREA_OF_CITY AS area_of_city,
    ECO_BIOME_ AS ecosystem_biome_reference
FROM
    {{ source('dropbox', 'resolve_city_to_ecosystem') }}