{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_rowcount('zuora', 'rate_plan_charge', 227000) }}
