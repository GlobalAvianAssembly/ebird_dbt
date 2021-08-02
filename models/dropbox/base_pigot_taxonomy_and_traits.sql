SELECT
    REPLACE(Binomial, '_', ' ') AS scientific_name,
    STRUCT(
        PC1 AS pc1,
        PC2 AS pc2,
        PC3 AS pc3,
        PC4 AS pc4,
        PC5 AS pc5,
        PC6 AS pc6,
        PC7 AS pc7,
        PC8 AS pc8,
        PC9 AS pc9
    ) AS body_morphspace,
    STRUCT(
        Beak_PC1 AS pc1,
        Beak_PC2 AS pc2,
        Beak_PC3 AS pc3,
        Beak_PC4 AS pc4
    ) AS beak_morphspace,
    Realm AS realm,
    TrophicLevel AS trophic_level,
    TrophicNiche AS trophic_niche,
    ForagingNiche AS foraging_niche
FROM
    {{ source('dropbox', 'pigot_taxonomy_and_traits') }}