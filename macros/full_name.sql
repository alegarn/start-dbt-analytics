{%- macro concat_name(first_name, last_name) -%}
    CONCAT( {{ first_name }}, ' ', {{ last_name}} ) as full_name
{% endmacro -%}