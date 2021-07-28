{% macro clean_na(column_name) %}
    CASE {{ column_name }}
        WHEN 'NA' THEN NULL
        ELSE {{ column_name }}
    END
{% endmacro %}