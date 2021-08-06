{{ config(materialized='table') }}

{%- set number_of_random_samples = var('number_of_times_to_sample') -%}

{%- set fetch_localities_query -%}
    SELECT locality_id FROM {{ ref('urban_hotspot') }}
{%- endset -%}

{%- set results = run_query(fetch_localities_query) -%}

{%- if execute -%}
    {%- set results_list = results.columns[0].values() -%}
    {%- for locality_id in results_list -%}
        {%- if not loop.first %} UNION ALL {% endif -%}
        {%- for i in range(number_of_random_samples) -%}
            {%- if not loop.first %} UNION ALL {% endif -%}
            SELECT * FROM {{target.schema}}.sample_random_checklists_at_locality('{{ locality_id }}', {{ i }})
        {%- endfor -%}
    {%- endfor -%}
{%- endif -%}