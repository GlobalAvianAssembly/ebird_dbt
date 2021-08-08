{% macro create_stored_procedures() %}
    create schema if not exists {{target.schema}};

    {{ create_sample_locality_procedure() }};

    {{ create_all_samples_at_locality_procedure() }};
{% endmacro %}

{% macro create_sample_locality_procedure() %}
    CREATE OR REPLACE TABLE FUNCTION {{target.schema}}.sample_random_checklists_at_locality(input_locality_id STRING, input_sample_id INT64)
    AS
    WITH
        random_sample_of_checklists AS (
            SELECT
                checklist_id
            FROM {{ ref('int_checklist') }}
            WHERE locality_id = input_locality_id
            ORDER BY RAND()
            LIMIT {{ var('checklist_random_sample_size') }}
        ),
        sample AS (
            SELECT
                input_sample_id AS sample_id,
                scientific_name,
                common_name,
                ROUND(COUNT(*) / {{ var('checklist_random_sample_size') }} * 100, 2) AS percentage_of_checklists,
                input_locality_id AS locality_id
            FROM {{ ref('observations_filtered') }}
            JOIN random_sample_of_checklists USING(checklist_id)
            GROUP BY scientific_name, common_name
         )
        SELECT * FROM sample WHERE {{ appears_on_required_percentage_of_checklists('percentage_of_checklists') }}
{% endmacro %}


{% macro create_all_samples_at_locality_procedure() %}
    CREATE OR REPLACE TABLE FUNCTION {{target.schema}}.all_samples_of_random_checklists_at_locality(input_locality_id STRING)
    AS
    {% for i in range(var('number_of_times_to_sample')) -%}
        {%- if not loop.first %} UNION ALL {% endif -%}
        SELECT * FROM {{target.schema}}.sample_random_checklists_at_locality(input_locality_id, {{ i }})
   {%- endfor -%}
{% endmacro %}
