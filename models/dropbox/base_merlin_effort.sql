SELECT DISTINCT
    CITY_NAME AS city_name,
    STRUCT(
        NUMBER_OF_CHECKLISTS_MIN AS min,
        NUMBER_OF_CHECKLISTS_MAX AS max,
        NUMBER_OF_CHECKLISTS_AVG AS average
    ) AS checklists,
    STRUCT(
        PRECISION_MIN_KM AS min,
        PRECISION_MAX_KM AS max,
        PRECISION_AVG_KM AS average
    ) AS precision,
    NUMBER_OF_INVALID_PERIODS AS invalid_periods
FROM
    {{ source('dropbox', 'merlin_effort') }}