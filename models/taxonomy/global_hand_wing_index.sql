WITH hwi AS (
    SELECT
     *
    FROM {{ ref('base_catherineshread_hwi1_1') }}
), mapped_taxonomy AS (
    WITH hwi_taxonomy AS (
        SELECT DISTINCT scientific_name, CAST(NULL AS STRING) AS common_name FROM hwi
    ),
    {{ map_taxonomy('hwi_taxonomy') }}
)
SELECT
    mapped_taxonomy.ebird_scientific_name,
    mapped_taxonomy.ebird_common_name,
    hwi.*
FROM hwi
LEFT JOIN mapped_taxonomy USING (scientific_name)