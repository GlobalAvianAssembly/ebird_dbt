SELECT
    birdlife_urban_species.*,
    IF(EXISTS(
        SELECT scientific_name
        FROM {{ ref('regional_species') }} pool
        WHERE pool.scientific_name = birdlife_urban_species.scientific_name
            AND pool.city_id = birdlife_urban_species.city_id
            AND pool.present_in_birdlife_pool = TRUE
    ), TRUE, FALSE) AS present_in_regional_pool
FROM {{ ref('urban_species') }} birdlife_urban_species