{{ config(materialized='table') }}
WITH elton AS (
    SELECT * FROM {{ ref('base_elton_taxonomy') }}
),
pigot AS (
    SELECT * FROM {{ ref('base_pigot_taxonomy_and_traits') }}
),
joined AS (
    SELECT
        *
    FROM
        elton
    JOIN
        pigot USING (scientific_name)
)
SELECT * FROM joined