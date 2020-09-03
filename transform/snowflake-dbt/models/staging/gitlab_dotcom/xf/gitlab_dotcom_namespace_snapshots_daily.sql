{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH date_details AS (
  
    SELECT *
    FROM {{ ref("date_details") }}
     
), namespace_snapshots AS (

    SELECT
      *,
      IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
    FROM {{ ref('gitlab_dotcom_namespaces_snapshots_base') }}
    {% if is_incremental() %}
    WHERE dbt_scd_id in (
      SELECT base.dbt_scd_id
      FROM {{ ref('gitlab_dotcom_namespaces_snapshots_base') }} base
      WHERE NOT EXISTS
        (
          SELECT dbt_scd_id
          FROM {{this}} derived
          WHERE derived.dbt_scd_id = base.dbt_scd_id)
        )
    {% endif %}
  
), namespace_snapshots_daily AS (
  
    SELECT
      uuid_string() as primary_key,
      date_details.date_actual AS snapshot_day,
      namespace_snapshots.namespace_id,
      namespace_snapshots.plan_id,
      namespace_snapshots.parent_id,
      namespace_snapshots.owner_id,
      namespace_snapshots.namespace_type,
      namespace_snapshots.visibility_level,
      namespace_snapshots.shared_runners_minutes_limit,
      namespace_snapshots.extra_shared_runners_minutes_limit
    FROM namespace_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN namespace_snapshots.valid_from::DATE AND namespace_snapshots.valid_to_::DATE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_day, namespace_id ORDER BY valid_to_ DESC) = 1
  
)

SELECT *
FROM namespace_snapshots_daily
