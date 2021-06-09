WITH
birdlife_urban_species AS (
    SELECT DISTINCT
        hotspot_id,
        city_id,
        birdlife_scientific_name AS scientific_name,
        birdlife_common_name AS common_name
    FROM
        {{ ref('urban_species') }}
    JOIN
        {{ ref('used_ebird_taxonomy') }} USING (scientific_name)
)
SELECT
    birdlife_urban_species.*,
    IF(EXISTS(
        SELECT scientific_name
        FROM {{ ref('regional_species') }} pool
        WHERE pool.scientific_name = birdlife_urban_species.scientific_name
            AND pool.city_id = birdlife_urban_species.city_id
            AND pool.present_in_birdlife_pool = TRUE
    ), TRUE, FALSE) AS present_in_regional_pool
FROM birdlife_urban_species