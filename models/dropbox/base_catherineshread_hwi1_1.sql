SELECT
    Species_name AS scientific_name,
    {{ clean_na('IUCN_name') }} AS iucn_scientific_name,
    HWI AS hand_wing_index,
    HWI_2 AS sample_size,
    {{ clean_na('Body_mass_log') }} AS log_body_mass,
    {{ clean_na('Range_Size') }} AS range_size,
    {{ clean_na('Island') }} AS population_proportion_breeding_on_islands,
    STRUCT(
        {{ clean_na('Migration1') }} AS birdlife,
        {{ clean_na('Migration2') }} AS tobias,
        {{ clean_na('Migration3') }} AS eyres
    ) AS migration_scores,
    {{ clean_na('Territoriality') }} AS territoriality,
    {{ clean_na('Diet') }} AS diet,
    CASE Habitat
        WHEN '1' THEN 'dense'
        WHEN '2' THEN 'semi-open'
        WHEN '3' THEN 'open'
        WHEN 'NA' THEN NULL
    END AS preferred_habitat,
    STRUCT(
        {{ clean_na('Latitude') }} AS latitude,
        {{ clean_na('AnnualTemp') }} AS temperature,
        {{ clean_na('TempRange') }} AS temperature_range,
        {{ clean_na('AnnualPrecip') }} AS annual_precipitation,
        {{ clean_na('PrecipRange') }} AS precipitation_range
    ) AS mean_breeding_metrics
FROM {{ source('dropbox', 'catherinesheard_global_hwi1_1')}}