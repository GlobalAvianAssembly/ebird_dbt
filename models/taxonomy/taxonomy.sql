{{ config(materialized='table') }}
WITH used_taxonomy AS (
    SELECT * FROM {{ ref('distinct_used_taxonomy') }}
),
traits AS (
    SELECT * EXCEPT(common_name) FROM {{ ref('trait_database') }}
),
direct_mapped AS (
    SELECT
        *
    FROM used_taxonomy
    JOIN traits USING (scientific_name)
),
birdlife_mapped AS (
    SELECT
        used_taxonomy.*,
        traits.* EXCEPT(scientific_name)
    FROM used_taxonomy
    JOIN {{ ref('used_birdlife_taxonomy') }} birdlife ON used_taxonomy.scientific_name = birdlife.ebird_scientific_name
    JOIN traits ON traits.scientific_name = birdlife.scientific_name
    WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
),
birdlife_alternative_name_mapped AS (
    SELECT
        used_taxonomy.*,
        traits.* EXCEPT(scientific_name)
    FROM used_taxonomy
    JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife ON used_taxonomy.scientific_name = birdlife.scientific_name
    JOIN UNNEST(birdlife.alternative_scientific_names) AS alternative_scientific_name
    JOIN traits ON traits.scientific_name = alternative_scientific_name
    WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
    AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_mapped)
),
manually_mapped AS (
    SELECT
        used_taxonomy.*,
        traits.* EXCEPT(scientific_name)
    FROM used_taxonomy
    JOIN {{ ref('avibase_manually_mapped_taxonomy') }} manual ON used_taxonomy.scientific_name = manual.ebird_scientific_name
    JOIN traits ON traits.scientific_name = manual.alternative_scientific_name
    WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
    AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_mapped)
    AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_alternative_name_mapped)
)
SELECT * FROM direct_mapped
UNION ALL
SELECT * FROM birdlife_mapped
UNION ALL
SELECT * FROM birdlife_alternative_name_mapped
UNION ALL
SELECT * FROM manually_mapped