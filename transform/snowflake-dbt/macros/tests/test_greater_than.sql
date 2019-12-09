{% macro greater_than(model, comparison_column_name, column_name) %}

WITH data AS (

    SELECT
      {{ column_name }}            AS greater_column,
      {{ comparison_column_name }} AS lesser_column,
    FROM {{ model }}

)

SELECT COUNT(*)
FROM data
WHERE greater_column < lesser_column

{% endmacro %}
