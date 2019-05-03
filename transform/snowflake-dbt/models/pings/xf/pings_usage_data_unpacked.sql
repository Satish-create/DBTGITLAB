{{
  config(
    materialized='incremental',
    unique_key='id', 
    schema='analytics'
  )
}}

{% set ping_list = dbt_utils.get_column_values(table=ref('pings_list'), column='full_ping_name', max_records=1000) %}

WITH usage_data as (

    SELECT * FROM {{ ref('pings_usage_data') }}
)

SELECT  id,
        source_ip,
        version,
        installation_type,
        active_user_count,
        created_at,
        mattermost_enabled,
        uuid,
        CASE WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
                THEN 'SaaS'
             ELSE 'Self-Hosted'
        END AS ping_source,
        edition,
        concat(concat(SPLIT_PART(version, '.', 1), '.'), SPLIT_PART(version, '.', 2))   AS major_version,
        CASE WHEN version LIKE '%ee%' THEN 'EE'
          ELSE 'CE' END                                                     AS main_edition,
        CASE WHEN edition LIKE '%CE%' THEN 'Core'
            WHEN edition LIKE '%EES%' THEN 'Starter'
            WHEN edition LIKE '%EEP%' THEN 'Premium'
            WHEN edition LIKE '%EEU%' THEN 'Ultimate'
            WHEN edition LIKE '%EE Free%' THEN 'Core'
            WHEN edition LIKE '%EE%' THEN 'Starter'
          ELSE null END                                                     AS edition_type,
        hostname,
        host_id,

        {% for ping_name in ping_list %}
        stats_used['{{ping_name}}']::numeric                                AS {{ping_name}} {{ "," if not loop.last }}
        {% endfor %}

FROM usage_data

{% if is_incremental() %}
    WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
