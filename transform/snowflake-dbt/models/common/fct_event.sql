{{ config(
    tags=["mnpi_exception", "product"],
    materialized = "incremental",
    unique_key = "event_id"
) }}

{{ simple_cte([
<<<<<<< HEAD
    ('dim_date', 'dim_date'),
    ('prep_event_all', 'prep_event_all')
=======
    ('dim_date', 'dim_date')
>>>>>>> 59c363352 (convert to incremental)
    ])
}},

fct_events AS (

  SELECT
    prep_event_all.event_id,
    prep_event_all.event_name,
    prep_event_all.ultimate_parent_namespace_id,
    prep_event_all.dim_user_id,
    prep_event_all.parent_type,
    prep_event_all.parent_id,
    prep_event_all.dim_project_id,
    prep_event_all.event_created_at,
<<<<<<< HEAD
    prep_event_all.plan_was_paid_at_event_timestamp,
    prep_event_all.plan_id_at_event_timestamp,
    prep_event_all.plan_name_at_event_timestamp,
=======
>>>>>>> 59c363352 (convert to incremental)
    prep_event_all.days_since_user_creation_at_event_date,
    prep_event_all.days_since_namespace_creation_at_event_date,
    prep_event_all.days_since_project_creation_at_event_date,
    CAST(prep_event_all.event_created_at AS DATE) AS event_date
<<<<<<< HEAD
  FROM prep_event_all
  
  {% if is_incremental() %}
=======
  FROM {{ ref('prep_event_all') }}
>>>>>>> 59c363352 (convert to incremental)

   WHERE event_created_at > (SELECT DATEADD(DAY, -30 , max(event_created_at)) FROM {{ this }})

  {% endif %}

<<<<<<< HEAD
=======
  SELECT
    ultimate_parent_namespace_id,
    plan_was_paid_at_event_date,
    plan_id_at_event_date,
    plan_name_at_event_date,
    event_created_at,
    CAST(event_created_at AS DATE) AS event_date
  FROM prep_event_all
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ultimate_parent_namespace_id, event_date
      ORDER BY event_created_at DESC) = 1

),

final AS (

  SELECT
    fct_events.*,
    paid_flag_by_day.plan_was_paid_at_event_date,
    paid_flag_by_day.plan_id_at_event_date,
    paid_flag_by_day.plan_name_at_event_date
  FROM fct_events
  LEFT JOIN paid_flag_by_day
    ON fct_events.ultimate_parent_namespace_id = paid_flag_by_day.ultimate_parent_namespace_id
      AND CAST(fct_events.event_created_at AS DATE) = paid_flag_by_day.event_date
>>>>>>> 59c363352 (convert to incremental)

),

gitlab_dotcom_fact AS (

  SELECT
    --Primary Key
    fct_events.event_id,
    
    --Foreign Keys
    dim_date.date_id AS dim_event_date_id,
<<<<<<< HEAD
    fct_events.ultimate_parent_namespace_id AS dim_ultimate_parent_namespace_id,
    fct_events.dim_project_id,
    fct_events.dim_user_id,
=======
    final.ultimate_parent_namespace_id AS dim_ultimate_parent_namespace_id,
    final.dim_project_id,
    final.dim_user_id,
>>>>>>> 59c363352 (convert to incremental)
    
    --Time attributes
    fct_events.event_created_at,
    fct_events.event_date,
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
<<<<<<< HEAD
    fct_events.parent_id,
    fct_events.parent_type,
    fct_events.event_name,
    fct_events.plan_id_at_event_timestamp,
    fct_events.plan_name_at_event_timestamp,
    fct_events.plan_was_paid_at_event_timestamp,
    fct_events.days_since_user_creation_at_event_date,
    fct_events.days_since_namespace_creation_at_event_date,
    fct_events.days_since_project_creation_at_event_date,
=======
    final.parent_id,
    final.parent_type,
    final.event_name,
    final.plan_id_at_event_date,
    final.plan_name_at_event_date,
    final.plan_was_paid_at_event_date,
    final.days_since_user_creation_at_event_date,
    final.days_since_namespace_creation_at_event_date,
    final.days_since_project_creation_at_event_date,
>>>>>>> 59c363352 (convert to incremental)
    'GITLAB_DOTCOM' AS data_source
  FROM fct_events
  LEFT JOIN dim_date
    ON fct_events.event_date = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="gitlab_dotcom_fact",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-01-20",
<<<<<<< HEAD
    updated_date="2022-06-06"
=======
    updated_date="2022-05-18"
>>>>>>> 59c363352 (convert to incremental)
) }}
