WITH taxonomy AS (
    SELECT * FROM {{ source('dropbox', 'birdlife_taxonomy') }}
)
SELECT DISTINCT
    (SELECT common_name FROM taxonomy WHERE t.scientific_name = scientific_name AND common_name IS NOT NULL) AS common_name,
    scientific_name
FROM
    taxonomy t