{{ config({
    "tags": ["tdf"]
    })
}}

{{ source_column_existence(
    'zuora_stitch', 
    'account',
    ['name', 'id']
) }}
