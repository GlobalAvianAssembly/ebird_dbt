{% macro landcover_struct(column_prefix) %}
        STRUCT(
            {{ column_prefix }}_pc_bare AS bare,
            STRUCT(
                {{ column_prefix }}_pc_closed_forest_deciduous_broadleaf AS deciduous_broadleaf,
                {{ column_prefix }}_pc_closed_forest_deciduous_needle AS deciduous_needle,
                {{ column_prefix }}_pc_closed_forest_evergreen_broadleaf AS evergreen_broadleaf,
                {{ column_prefix }}_pc_closed_forest_evergreen_needle AS evergeen_needle,
                {{ column_prefix }}_pc_closed_forest_forest_mixed AS mixed,
                {{ column_prefix }}_pc_closed_forest_forest_other AS other
            ) AS closed_forest_cover,
            {{ column_prefix }}_pc_closed_forest_forest_total AS closed_forest_total,
            {{ column_prefix }}_pc_cultivated AS cultivated,
            {{ column_prefix }}_pc_herbaceous_vegetation AS herbaceous_vegetation,
            {{ column_prefix }}_pc_herbaceous_wetland AS herbaceous_wetland,
            {{ column_prefix }}_pc_moss_and_lichen AS moss_and_lichen,
            {{ column_prefix }}_pc_ocean AS ocean,
            STRUCT(
                {{ column_prefix }}_pc_open_forest_deciduous_broadleaf AS deciduous_broadleaf,
                {{ column_prefix }}_pc_open_forest_deciduous_needle AS deciduous_needle,
                {{ column_prefix }}_pc_open_forest_evergreen_broadleaf AS evergreen_broadleaf,
                {{ column_prefix }}_pc_open_forest_evergreen_needle AS evergeen_needle,
                {{ column_prefix }}_pc_open_forest_forest_mixed AS mixed,
                {{ column_prefix }}_pc_open_forest_forest_other AS other
            ) AS open_forest_cover,
            {{ column_prefix }}_pc_open_forest_forest_total AS open_forest_total,
            {{ column_prefix }}_pc_permanent_water AS permanent_water,
            {{ column_prefix }}_pc_shrubs AS shrubs,
            {{ column_prefix }}_pc_snow AS snow,
            {{ column_prefix }}_pc_unknown AS unknown,
            {{ column_prefix }}_pc_urban AS urban
        )
{% endmacro %}