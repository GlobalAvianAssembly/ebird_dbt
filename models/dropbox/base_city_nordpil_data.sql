SELECT
    City AS name,
    Latitude AS latitude,
    Longitude AS longitude,
FROM {{ source('dropbox', 'nordpil_urbanareas1_1')}}
WHERE City IN (SELECT CITY_NAME FROM {{ source('dropbox', 'urban_hotspots') }})