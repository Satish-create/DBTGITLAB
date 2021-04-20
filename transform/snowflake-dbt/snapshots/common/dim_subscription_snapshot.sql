{% snapshot fct_mrr_snapshot %}

    {{
        config(
          unique_key='dim_subscription_id',
          strategy='check',
          check_cols=['subscription_status', 'is_auto_renew']
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('dim_subscription'),
            except=['DBT_UPDATED_AT', 'DBT_CREATED_AT']
            )
      }}
    FROM {{ ref('dim_subscription') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_subscription_id ORDER BY subscription_start_date DESC) = 1

{% endsnapshot %}
