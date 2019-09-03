{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('customers', 'customers_db_orders') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER                                     AS order_id,
    customer_id::INTEGER                            AS customer_id,
    product_rate_plan_id::VARCHAR                   AS product_rate_plan_id,
    subscription_id::VARCHAR                        AS subscription_id,
    subscription_name::VARCHAR                      AS subscription_name,
    {{zuora_slugify("subscription_name")}}::VARCHAR AS subscription_name_slugify,
    start_date::TIMESTAMP                           AS order_start_date,
    end_date::TIMESTAMP                             AS order_end_date,
    quantity::INTEGER                               AS order_quantity,
    created_at::TIMESTAMP                           AS order_created_at,
    updated_at::TIMESTAMP                           AS order_updated_at,
    NULLIF(gl_namespace_id, '')::INTEGER            AS gitlab_namespace_id,
    NULLIF(gl_namespace_name, '')::VARCHAR          AS gitlab_namespace_name,
    amendment_type::VARCHAR                         AS amendment_type,
    trial::BOOLEAN                                  AS order_is_trial,
    last_extra_ci_minutes_sync_at::TIMESTAMP        AS last_extra_ci_minutes_sync_at
  FROM source
  WHERE rank_in_key = 1
)

SELECT
  *
FROM renamed
