{%- macro apply_dynamic_data_masking(database, schema, table_name, table_type, column_name, data_type, policy ) -%}



alter {{table_type}} "{{database}}".{{schema}}.{{table_name}} 
modify column {{ column_name }} 
set masking policy "{{database}}".{{schema}}.{{ policy }}_{{ data_type }};
        

{%- endmacro -%}
