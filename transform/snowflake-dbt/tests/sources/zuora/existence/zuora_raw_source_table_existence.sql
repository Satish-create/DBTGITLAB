{{ config({
    "tags": ["tdf"]
    })
}}

{{ source_table_existence(
    'zuora_stitch', 
    ['account', 'subscription', 'rateplancharge']
) }}
