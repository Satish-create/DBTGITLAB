{% set names = ['pings_usage_data_version', 'pings_usage_data_tappg'] %}

with {% for name in names %} {{name}} as (
    SELECT *
    FROM {{ref(name)}}
), {%- endfor -%} unioned as (

{%- for name in names -%} 
    SELECT * 
    FROM {{name}}
    {% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}

)

SELECT * 
FROM unioned
{{ dbt_utils.group_by(n=22) }}
