SELECT
    traits.*,
    iucn_red_list_2020
FROM
    {{ ref('taxonomy_with_traits') }} traits
JOIN {{ ref('taxonomy_with_vulnerability') }} USING (scientific_name)