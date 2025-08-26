{% test no_hash_collisions(model, column_name,hashed_fields) %}
WITH

all_tuples AS (
    SELECT DISTINCT {{column_name}} AS HASH, {{hashed_fields}} FROM {{model}}
),

validation_errors AS (
    SELECT HASH, COUNT(*) FROM all_tuples GROUP BY HASH HAVING COUNT(*) > 1
)

SELECT * FROM validation_errors

{%- endtest%}

{% macro as_sql_list(hashed_fields_list) -%}
{{ hashed_fields_list|join(', ') }}
{%- endmacro%}