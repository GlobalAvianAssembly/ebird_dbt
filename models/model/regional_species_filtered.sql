SELECT
    species.*
FROM {{ ref('regional_species') }} species
JOIN {{ ref('taxonomy') }} definition USING (scientific_name)
WHERE
    is_pelagic_specialist = FALSE
    AND
    iucn_red_list_2020 IN ('LC', 'NT', 'VU', 'EN', 'CR', 'DD', 'NR')
