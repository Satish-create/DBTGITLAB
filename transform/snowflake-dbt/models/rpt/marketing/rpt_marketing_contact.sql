WITH mart_marketing_contact AS (
  
    SELECT
      {{ dbt_utils.star(from=ref('mart_marketing_contact'), except=['EMAIL_ADDRESS', 'FIRST_NAME', 'LAST_NAME', 'GITLAB_USER_NAME', 'GITLAB_DOTCOM_USER_ID',
      'CREATED_BY', 'UPDATED_BY', 'CREATED_DATE', 'UPDATED_DATE']) }}
    FROM {{ ref('mart_marketing_contact') }}

)

{{ dbt_audit(
    cte_ref="mart_marketing_contact",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-05-13",
    updated_date="2021-05-13"
) }}
