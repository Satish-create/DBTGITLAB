{% snapshot sheetload_comp_band_snapshots %}

    {{
        config(
          unique_key='"employee_number"',
          strategy='timestamp',
          updated_at='_UPDATED_AT',
          enabled=False
        )
    }}

    SELECT *
    FROM {{ source('sheetload', 'comb_band') }}
    WHERE "Employee_ID" != ''

{% endsnapshot %}

