SELECT DISTINCT
    city_name,
    ebird_code,
    common_name,
    scientific_name,
    number_of_non_zero_frequency,
    longest_run_of_non_zero_frequency,
    smallest_precision
FROM
    {{ source('dropbox', 'merlin_frequencies') }}
WHERE species_category = 'species'