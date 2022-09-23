{% macro grant_select(schema = target.schema, role = target.role) %}
    
    {% set query %}
        grant usage on schema {{ schema }} to role {{ role }}
        grant usage on all tables in schema {{ schema }} to role {{ role }}
        grant usage on all views in schema {{ schema }} to role {{ role }}
    {% endset %}

    {{ log('Grant usage privileges on schema ' ~schema ~' to role ' ~role ~' .', info=True) }}
    {% do run_query(query) %}
    {{ log('Privileges done', info=True)}}
{% endmacro %}