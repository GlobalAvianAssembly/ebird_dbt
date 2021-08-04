SELECT DISTINCT
    Common_name AS common_name,
    Scientific_name AS scientific_name,
    Alternative_common_names AS alternative_common_names,
    Synonyms AS alternative_scientific_names,
    IUCN_Red_List_category_2020 AS iucn_red_list_2020,
    CASE IUCN_Red_List_category_2020
        WHEN 'LC' THEN 1
        WHEN 'NT' THEN 2
        WHEN 'VU' THEN 3
        WHEN 'EN' THEN 4
        WHEN 'CR' THEN 5
        WHEN 'CR (PE)' THEN 6
        WHEN 'EW' THEN 7
        WHEN 'EX' THEN 8
        WHEN 'DD' THEN 9
        WHEN 'NR' THEN 10
        ELSE 11
    END AS iucn_red_list_order
FROM
    {{ source('dropbox', 'birdlife_taxonomy_v5') }}
WHERE Subspp_Seq = 0