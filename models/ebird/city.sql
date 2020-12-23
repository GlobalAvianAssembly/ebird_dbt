SELECT
    row_number() OVER () AS city_id,
    CITY_NAME AS name,
    ROUND(CAST(POP_2015 AS NUMERIC), 0) AS population_2015,
    ROUND(CAST(BU_2015 AS NUMERIC), 0) AS built_area_2015,
    MIN(ELEVATION) AS min_hotspot_elevation,
    MAX(ELEVATION) max_hotspot_elevation
FROM dropbox.urban_hotspots
GROUP BY CITY_NAME, POP_2015, BU_2015
ORDER BY POP_2015 DESC