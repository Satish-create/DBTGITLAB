{% snapshot zuora_revenue_approval_detail_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='rc_id',
          updated_at='incr_updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue', 'zuora_revenue_approval_detail') }}

{% endsnapshot %}
