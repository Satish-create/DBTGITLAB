{%- macro create_masking_policy(database, schema, data_type, policy) -%}

  {% if data_type == 'TEXT' %}
    {% set mask = '\'***MASKED***\''%}
  {% elif data_type == 'TIMESTAMP_NTZ' %}
    {% set mask = 'NULL'%}
  {% elif data_type == 'ARRAY' %}
    {% set mask = '[\'***MASKED***\']'%}
  {% elif data_type == 'VARIANT' %}
    {% set mask = '[\'{***MASKED***}\']'%}
  {% elif data_type == 'DATE' %}
    {% set mask = 'NULL'%}
  {% elif data_type == 'FLOAT' %}
    {% set mask = '0.0'%}
  {% elif data_type == 'NUMBER' %}
    {% set mask = '0'%}
  {% elif data_type == 'BOOLEAN' %}
    {% set mask = 'NULL'%}
  {% else %}
    {% set mask = 'NULL'%}
  {% endif %}

CREATE MASKING POLICY IF NOT EXISTS "{{ database }}".{{ schema }}.{{ policy }}_{{ data_type }} AS (val {{ data_type }}) 
  RETURNS {{ data_type }} ->
      CASE WHEN CURRENT_ROLE() IN ('transformer') THEN val  -- Set for specific roles that should always have access
      CASE WHEN IS_ROLE_IN_SESSION('{{ policy }}') THEN val -- Set for the user to inherit access bases on there roles
      ELSE {{ mask }} 
      END; 

{%- endmacro -%}
