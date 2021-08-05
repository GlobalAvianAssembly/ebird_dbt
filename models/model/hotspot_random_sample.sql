{{ config(materialized='table') }}

{% set checklist_sample_size = 15 %}

{% for i in range(100) %}
    {% if not loop.first %} UNION ALL {% endif %}
    (
        WITH sample AS (
            SELECT DISTINCT
                '{{ i }}' AS sample_id,
                scientific_name,
                common_name,
                ROUND(COUNT(*) / {{ checklist_sample_size }} * 100, 2) AS percentage_of_checklists,
                'L5422255' AS locality_id
            FROM {{ ref('observations_filtered') }}
            WHERE checklist_id IN
                (SELECT checklist_id FROM {{ ref('int_checklist') }} WHERE locality_id = 'L5422255' ORDER BY RAND() LIMIT {{ checklist_sample_size }})
            GROUP BY scientific_name, common_name
         )
         SELECT * FROM sample WHERE {{ appears_on_required_percentage_of_checklists('percentage_of_checklists') }}
    )
{% endfor %}