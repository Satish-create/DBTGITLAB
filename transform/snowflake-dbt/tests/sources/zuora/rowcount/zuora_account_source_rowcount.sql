{{ config({
    "tags": ["tdf"]
    })
}}

{{ source_rowcount('zuora', 'account', 26000) }}
