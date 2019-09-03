{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'gitlab_subscriptions') }}
  WHERE id != 572635 -- This ID has NULL values for many of the important columns.

), renamed AS (

    SELECT
      id :: integer                                   AS gitlab_subscription_id,
      start_date :: date                              AS gitlab_subscription_start_date,
      end_date :: date                                AS gitlab_subscription_end_date,
      trial_ends_on :: date                           AS gitlab_subscription_trial_ends_on,
      namespace_id :: integer                         AS namespace_id,
      hosted_plan_id :: integer                       AS plan_id,
      max_seats_used :: integer                       AS max_seats_used,
      seats :: integer                                AS seats,
      trial :: boolean                                AS is_trial,
      created_at :: timestamp                         AS gitlab_subscription_created_at,
      updated_at :: timestamp                         AS gitlab_subscription_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
