{% test has_no_missing_species(model, column_name, all_species_model, all_species_field) %}

select * from {{ all_species_model }}
where {{ all_species_field }} not in (select {{ column_name }} from {{ model }})

{% endtest %}