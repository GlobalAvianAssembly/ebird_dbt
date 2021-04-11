SELECT DISTINCT
    Common_name AS common_name,
    Scientific_name AS scientific_name,
    Alternative_common_names AS alternative_common_names,
    Synonyms AS alternative_scientific_names,
    IUCN_Red_List_category_2020 AS iucn_red_list_2020
FROM
    {{ source('dropbox', 'birdlife_taxonomy_v5') }}
WHERE Subspp_Seq = 0