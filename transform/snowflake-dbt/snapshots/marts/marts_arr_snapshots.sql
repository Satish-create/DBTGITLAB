{% snapshot marts_arr_snapshots %}

    {{
        config(
          unique_key='primary_key',
          strategy='check',
          check_cols=['mrr', 'arr', 'quantity']
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('mart_arr'),
            except=['DBT_UPDATED_AT', 'DBT_CREATED_AT']
            )
      }}
    FROM {{ ref('mart_arr') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY arr_month DESC) = 1

{% endsnapshot %}
