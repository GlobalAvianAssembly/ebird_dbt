SELECT
    city_id,
    common_name,
    scientific_name,
    number_of_non_zero_frequency,
    longest_run_of_non_zero_frequency,
    smallest_precision
FROM
    {{ ref('base_merlin_species_pool') }} regional_pool
JOIN {{ ref('city') }} city
    ON regional_pool.city_name = city.name
