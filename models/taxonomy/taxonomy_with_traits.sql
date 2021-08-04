{{ config(materialized='table') }}
WITH used_taxonomy AS (
    SELECT * FROM {{ ref('used_taxonomy_all') }}
),
traits AS (
    SELECT * EXCEPT(common_name) FROM {{ ref('trait_database') }}
),
direct_mapped AS (
    {# used_taxonomy.scientific_name = traits.scientific_name #}
    SELECT
        *
    FROM used_taxonomy
    JOIN traits USING (scientific_name)
),
birdlife_mapped AS (
    {# used_taxonomy.scientific_name = (birdlife.ebird_scientific_name | birdlife.scientific_name) = traits.scientific_name #}
    SELECT
        used_taxonomy.*,
        traits.* EXCEPT(scientific_name)
    FROM used_taxonomy
    JOIN {{ ref('used_birdlife_taxonomy') }} birdlife ON used_taxonomy.scientific_name = birdlife.ebird_scientific_name
    JOIN traits ON traits.scientific_name = birdlife.scientific_name
    WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
),
birdlife_alternative_name_mapped AS (
    {# used_taxonomy.scientific_name = (birdlife.ebird_scientific_name | birdlife.alternative_scientific_name) = traits.scientific_name #}
    SELECT
        used_taxonomy.*,
        traits.* EXCEPT(scientific_name),
        row_number() OVER (PARTITION BY used_taxonomy.scientific_name ORDER BY RAND()) AS rownum
    FROM used_taxonomy
    JOIN {{ ref('birdlife_taxonomy_with_alternatives') }} birdlife ON used_taxonomy.scientific_name = birdlife.scientific_name
    JOIN UNNEST(birdlife.alternative_scientific_names) AS alternative_scientific_name
    JOIN traits ON traits.scientific_name = alternative_scientific_name
    WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
        AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_mapped)
),
manually_mapped AS (
    {# used_taxonomy.scientific_name = (manual.ebird_scientific_name | manual.alternative_scientific_name) = traits.scientific_name #}
    SELECT
        used_taxonomy.*,
        traits.* EXCEPT(scientific_name)
    FROM used_taxonomy
    JOIN {{ ref('avibase_manually_mapped_taxonomy') }} manual ON used_taxonomy.scientific_name = manual.ebird_scientific_name
    JOIN traits ON traits.scientific_name = manual.alternative_scientific_name
    WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
        AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_mapped)
        AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_alternative_name_mapped)
),
manually_mapped2 AS (
     {# used_taxonomy.scientific_name = (manual2.ebird_scientific_name | manual2.birdlife_v4_2010_scientific_name) = traits.scientific_name #}
     SELECT
         used_taxonomy.*,
         traits.* EXCEPT(scientific_name)
     FROM used_taxonomy
     JOIN {{ ref('avibase_manually_mapped_taxonomy_for_traits') }} manual2 ON used_taxonomy.scientific_name = manual2.ebird_scientific_name
     JOIN traits ON traits.scientific_name = manual2.birdlife_v4_2010_scientific_name
     WHERE used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM direct_mapped)
         AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_mapped)
         AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM birdlife_alternative_name_mapped)
         AND used_taxonomy.scientific_name NOT IN (SELECT scientific_name FROM manually_mapped)
)
SELECT * FROM direct_mapped
UNION ALL
SELECT * FROM birdlife_mapped
UNION ALL
SELECT * EXCEPT(rownum) FROM birdlife_alternative_name_mapped WHERE rownum = 1
UNION ALL
SELECT * FROM manually_mapped
UNION ALL
SELECT * FROM manually_mapped2