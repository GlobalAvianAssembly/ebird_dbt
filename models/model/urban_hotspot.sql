WITH
included_hotspot_richness AS (
    SELECT
        hotspot_id,
        COUNT(*) AS richness
    FROM {{ ref('urban_species' )}}
    GROUP BY hotspot_id
),
total_hotspot_richness AS (
    SELECT
        hotspot_id,
        COUNT(*) AS richness
    FROM {{ ref('int_species_at_hotspot' )}}
    GROUP BY hotspot_id
),
chao2 AS (
    SELECT
        hotspot_id,
        locality_id,
        name,
        latitude,
        longitude,
        city_id,
        elevation,
        number_of_checklists,
        ihr.richness AS project_richness,
        thr.richness AS total_richness,
        (SELECT COUNT(*) FROM {{ ref('int_species_at_hotspot') }} WHERE number_of_checklist_appearances = 1 AND hotspot_id = hs.hotspot_id) AS one_observation,
        (SELECT COUNT(*) FROM {{ ref('int_species_at_hotspot') }} WHERE number_of_checklist_appearances = 2 AND hotspot_id = hs.hotspot_id) AS two_observations
    FROM {{ ref('eph_included_hotspot') }} hs
    JOIN included_hotspot_richness ihr USING (hotspot_id)
    JOIN total_hotspot_richness thr USING (hotspot_id)
    WHERE {{ is_urban() }}
)
SELECT
    * EXCEPT (one_observation, two_observations),
    CASE
        WHEN two_observations > 0
        THEN ROUND(total_richness + ((one_observation * one_observation) / (2 * two_observations)), 1)
        ELSE -1
    END AS chao_estimate
FROM chao2