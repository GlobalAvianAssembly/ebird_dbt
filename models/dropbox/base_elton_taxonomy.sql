SELECT
    common_name AS common_name,
    scientific_name AS scientific_name,
    STRUCT(
        diet_invertebrate AS invertebrate,
        diet_vert_endo AS vert_endo,
        diet_vert_ecto AS vert_ecto,
        diet_vert_fish AS vert_fish,
        diet_vert_unknown AS vert_unknown,
        diet_scavenger AS scavenger,
        diet_fruit AS fruit,
        diet_nectar AS nectar,
        diet_seed AS seed,
        diet_other_plant AS other_plant,
        diet_category AS category,
        diet_certainty AS certainty
    ) AS diet,
    STRUCT(
        foraging_below_water AS below_water,
        foraging_around_surf AS around_surf,
        foraging_ground AS ground,
        foraging_understory AS understory,
        foraging_midheight AS midheight,
        foraging_canopy AS canopy,
        foraging_aerial AS aerial
    ) AS foraging,
    is_noctural,
    body_mass_value,
    is_pelagic_specialist
FROM
    {{ source('dropbox', 'elton_traits') }}