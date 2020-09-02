{% macro source_column_sum_min(schema, table, column, min_value, where_clause=None) %}

WITH source as (

    SELECT *
    FROM {{ source(schema, table) }}

), counts AS (

    SELECT sum({{column}}) as sum_value
    FROM source
    {% if where_clause != None %}
    WHERE {{ where_clause }}
    {% endif %}

)

SELECT sum_value
FROM counts
WHERE sum_value < {{ min_value }}

{% endmacro %}
