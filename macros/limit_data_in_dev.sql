{%- macro limit_data_in_dev(add_column, time_data) -%}
    {% if target.name == 'dev' -%}
        where {{ add_column }} >= dateadd('day', -{{ time_data }}, current_timestamp)
    {%- endif %}
{% endmacro %}