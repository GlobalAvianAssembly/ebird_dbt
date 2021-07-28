SELECT DISTINCT
    eBird_species_code_2019 AS ebird_species_code,
    common_name,
    scientific_name,
    t_order,
    t_family
FROM
    {{ source('dropbox', 'ebird_clements_taxonomy_2019') }}
WHERE
    {{ is_included_ebird_species() }}
    AND
    extinct IS NULL