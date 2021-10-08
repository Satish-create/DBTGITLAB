{% snapshot zuora_revenue_revenue_contract_header_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='incr_updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_header') }}

{% endsnapshot %}
