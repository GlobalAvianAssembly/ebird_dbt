WITH taxonomy AS (
    SELECT * FROM {{ source('dropbox', 'birdlife_taxonomy') }}
), taxonomy_clean AS (
    SELECT DISTINCT
        (SELECT common_name FROM taxonomy WHERE t.scientific_name = scientific_name AND common_name IS NOT NULL) AS common_name,
        scientific_name
    FROM
        taxonomy t
)
SELECT DISTINCT
    regional_pool.binomial AS scientific_name,
    taxonomy_clean.common_name AS common_name,
    city.city_id AS city_id
FROM {{ source('dropbox', 'ee_birdlife_distribution_intersection_with_urban_area') }} regional_pool
LEFT JOIN taxonomy_clean
    ON regional_pool.binomial = taxonomy_clean.scientific_name
JOIN {{ ref('city') }} city
    ON regional_pool.city_name = city.name
