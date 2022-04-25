{%- macro create_masking_policy(database, schema, data_type, policy) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{ database }}".{{ schema }}.{{ policy }}_{{ data_type }} AS (val {{ data_type }}) 
  RETURNS {{ data_type }} ->
      CASE WHEN CURRENT_ROLE() IN ('transformer','{{ policy }}') THEN val 
      ELSE NULL
      END; 

{%- endmacro -%}
