--------------------------------------------------------
-- Macros defining eBird data to include
--------------------------------------------------------

--
-- Checklist protocols to include
--
{% macro is_required_protocol() %}
protocol_type IN ('Traveling', 'Stationary', 'Area')
{% endmacro %}

--
-- Locality types to include
--
{% macro is_required_hotspot_type() %}
 locality_type = 'H'
{% endmacro %}

--
-- Species observations to include
--
{% macro is_included_observation() %}
    approved = 1
    AND
    (
        category IN ('species', 'issf', 'form')
        OR
        (
            category = 'domestic' AND scientific_name = 'Columba livia'
        )
        OR
        (
            category = 'form' AND REGEXP_CONTAINS(scientific_name, '^[A-Z][a-z]+ [a-z]+$')
        )
    )
{% endmacro %}

--
-- Min and max elevation of hotspots in region around city, given min/max of hotspot elevation in city
--
{% macro elevation_is_within_bounds(col_elevation, col_min_urban_hotspot_elevation, col_max_urban_hotspot_elevation) %}
    {{ col_elevation }} < ({{ col_max_urban_hotspot_elevation }} + {{ var('max_difference_in_elevation_for_regional_hotspot') }})
AND {{ col_elevation }} > ({{ col_min_urban_hotspot_elevation }} - {{ var('max_difference_in_elevation_for_regional_hotspot') }})
{% endmacro %}

--
-- Number of checklists at locality required
--
{% macro has_required_number_of_checklists(col_checklist_count) %}
{{ col_checklist_count }} >= {{ var('min_number_of_checklists_required_at_hotspot') }}
{% endmacro %}

--
-- Percentage of checklists species must appear on to be included in study
--
{% macro appears_on_required_percentage_of_checklists(col_percentage_of_checklists) %}
{{ col_percentage_of_checklists }} >= {{ var('min_percentage_of_checklists_for_species_at_hotspot') }}
{% endmacro %}

--
-- Maximum distance travelled for a checklist to be included
--
{% macro effort_distance_is_within_bounds(col_effort_distance_km) %}
({{ col_effort_distance_km }} IS NULL OR {{ col_effort_distance_km }} <= {{ var('max_distance_travelled_for_checklist_km') }})
{% endmacro %}

--
-- Minimum and maximum duration for a checklist to be included
--
{% macro duration_minutes_is_within_bounds(col_duration_minutes) %}
{{ col_duration_minutes }} >= {{ var('min_duration_for_checklist') }}
AND {{ col_duration_minutes }} <= {{ var('max_duration_for_checklist') }}
{% endmacro %}

--
-- Earliest date for a checklist to be included
--
{% macro year_is_included(col_date) %}
EXTRACT(YEAR FROM {{ col_date }}) >= {{ var('min_year_for_inclusion') }}
{% endmacro %}