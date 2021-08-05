SELECT
    species.*
FROM {{ ref('regional_species') }} species
JOIN {{ ref('taxonomy') }} definition USING (scientific_name)
WHERE
    {{ filter_to_accepted_taxonomy() }}
