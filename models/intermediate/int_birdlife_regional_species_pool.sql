SELECT DISTINCT
    regional_pool.binomial AS scientific_name,
    t.common_name AS common_name,
    city.city_id AS city_id
FROM {{ source('dropbox', 'ee_birdlife_distribution_intersection_with_urban_area') }} regional_pool
LEFT JOIN {{ ref('int_birdlife_taxonomy') }} t
    ON regional_pool.binomial = t.scientific_name
JOIN {{ ref('city') }} city
    ON regional_pool.city_name = city.name
