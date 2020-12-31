WITH
checklists AS (
    SELECT
        sampling_event_identifier AS checklist_id,
        locality_id,
        number_observers,
        group_identifier,
        observation_date,
        time_observations_started,
        effort_distance_km,
        effort_area_ha,
        duration_minutes,
        protocol_type,
        row_number() OVER (PARTITION BY sampling_event_identifier ORDER BY global_unique_identifier) AS checklist_id_rownum
    FROM {{ source("dropbox", "ebird_2") }}
    WHERE all_species_reported = 1
        AND protocol_type IN ('Traveling', 'Stationary', 'Area')
        AND locality_id IN (SELECT locality_id FROM {{ ref("hotspot") }})
),
checklist_deduplicate AS (
  SELECT
    * EXCEPT (checklist_id_rownum, group_identifier),
    row_number() OVER (PARTITION BY group_identifier ORDER BY checklist_id) AS group_identifier_rownum
  FROM checklists
  WHERE checklist_id_rownum = 1
)
SELECT
    * EXCEPT (group_identifier_rownum)
FROM checklist_deduplicate
WHERE group_identifier_rownum = 1