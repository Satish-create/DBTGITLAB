{%- macro create_masking_policy(database, schema, data_type, policy) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{ database }}".{{ schema }}.{{ policy }}_{{ data_type }} AS (val {{ data_type }}) 
  RETURNS {{ data_type }} ->
      CASE WHEN CURRENT_ROLE() IN ('transformer') THEN val  -- Set for specific roles that should always have access
      CASE WHEN IS_ROLE_IN_SESSION('{{ policy }}') THEN val -- Set for the user to inherit access bases on there roles
      ELSE NULL
      END; 

{%- endmacro -%}
