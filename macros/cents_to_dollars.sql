{% macro cents_to_dollars(number) -%}
    round( 1.0 * {{ number }} / 100, 4)
{%- endmacro %}
