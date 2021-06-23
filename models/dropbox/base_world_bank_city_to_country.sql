SELECT
    NAME_MAIN AS name,
    FORMAL_EN AS country,
    CONTINENT AS continent,
    SUBREGION AS subregion,
    GDP_MD_EST AS gdp_estimate,
    ECONOMY AS economy_description,
    INCOME_GRP AS income_group,
    FIPS_10_ AS country_code
FROM
    {{ source('dropbox', 'world_bank_city_to_country') }}