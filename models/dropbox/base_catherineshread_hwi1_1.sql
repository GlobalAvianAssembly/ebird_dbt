SELECT
    Species_name AS scientific_name,
    IUCN_name AS iucn_scientific_name,
    HWI AS hand_wing_index,
    HWI_2 AS sample_size,
    Body_mass_log AS log_body_mass,
    Range_Size AS range_size,
    Island AS population_proportion_breeding_on_islands,
    STRUCT(
        Migration1 AS birdlife,
        Migration2 AS tobias,
        Migration3 AS eyres
    ) AS migration_scores,
    Territoriality AS territoriality,
    Diet AS diet,
    Habitat AS preferred_habitat,
    STRUCT(
        Latitude AS latitude,
        AnnualTemp AS temperature,
        TempRange AS temperature_range,
        AnnualPrecip AS annual_precipitation,
        PrecipRange AS precipitation_range
    ) AS mean_breeding_metrics
FROM {{ source('dropbox', 'catherinesheard_global_hwi1_1')}}