WITH birdlife_expanded AS (
    SELECT DISTINCT
        regional_pool.species AS scientific_name,
        t.common_name AS common_name,
        city.city_id AS city_id,
        CASE regional_pool.presence
            WHEN '1' THEN 'Extant'
            WHEN '2' THEN 'Probably Extant'
            WHEN '3' THEN 'Possibly Extant'
            WHEN '4' THEN 'Possibly Extinct'
            WHEN '5' THEN 'Extinct'
            WHEN '6' THEN 'Presence Uncertain'
            ELSE 'Unknown Code'
        END AS presence,
        regional_pool.presence AS presence_code,
        CASE regional_pool.seasonal
            WHEN '1' THEN 'Resident'
            WHEN '2' THEN 'Breeding Season'
            WHEN '3' THEN 'Non-breeding Season'
            WHEN '4' THEN 'Passage'
            WHEN '5' THEN 'Seasonal occurence uncertain'
            ELSE 'Unknown Code'
        END AS seasonal,
        regional_pool.seasonal AS seasonal_code,
         CASE regional_pool.origin
            WHEN '1' THEN 'Native'
            WHEN '2' THEN 'Reintroduced'
            WHEN '3' THEN 'Introduced'
            WHEN '4' THEN 'Vagrant'
            WHEN '5' THEN 'Origin Uncertain'
            WHEN '6' THEN 'Assisted Colonisation'
            ELSE 'Unknown Code'
        END AS origin,
        regional_pool.origin AS origin_code,
        t.iucn_red_list_2020 AS iucn_red_list_2020,
        CASE t.iucn_red_list_2020
            WHEN 'LC' THEN 1
            WHEN 'NT' THEN 2
            WHEN 'VU' THEN 3
            WHEN 'EN' THEN 4
            WHEN 'CR' THEN 5
            WHEN 'CR (PE)' THEN 6
            WHEN 'DD' THEN 7
            ELSE 8
        END AS iucn_red_list_order
    FROM {{ source('dropbox', 'ee_birdlife_distribution_intersection_with_urban_area') }} regional_pool
    LEFT JOIN {{ ref('base_birdlife_taxonomy') }} t
        ON regional_pool.species = t.scientific_name
    JOIN {{ ref('city') }} city
        ON regional_pool.city_name = city.name
    WHERE regional_pool.presence != '5'
), deduplicated AS (
    SELECT
        scientific_name,
        common_name,
        city_id,
        ARRAY(
            SELECT
                STRUCT(
                    presence,
                    presence_code,
                    seasonal,
                    seasonal_code,
                    origin,
                    origin_code
                )
            FROM birdlife_expanded sub
            WHERE core.scientific_name = sub.scientific_name
                AND core.common_name = sub.common_name
                AND core.city_id = sub.city_id
        ) AS presence,
        iucn_red_list_2020,
        iucn_red_list_order,
        row_number() OVER (PARTITION BY scientific_name, common_name, city_id) AS rownum
    FROM birdlife_expanded core
)
SELECT
    * EXCEPT (rownum)
FROM
    deduplicated
WHERE
    rownum = 1