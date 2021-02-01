WITH species_on_checklist AS (
  SELECT
    common_name,
    scientific_name,
    checklist_id,
    row_number() OVER (PARTITION BY common_name, scientific_name, checklist_id) AS rownum
  FROM {{ ref('int_observation') }}
), species_at_hotspot AS (
  SELECT
    locality_id,
    common_name,
    scientific_name,
    count(*) AS number_of_checklist_appearances
  FROM species_on_checklist
  JOIN {{ ref('int_checklist') }} USING (checklist_id)
  WHERE species_on_checklist.rownum = 1
  GROUP BY locality_id, common_name, scientific_name
)
SELECT
    hotspot_id,
    locality_id,
    city_id,
    common_name,
    scientific_name,
    number_of_checklist_appearances,
    number_of_checklists AS total_checklists_at_hotspot,
    ROUND(number_of_checklist_appearances / number_of_checklists * 100, 2) AS percentage_of_checklists
FROM {{ ref('eph_included_hotspot') }}
JOIN species_at_hotspot USING (locality_id)
