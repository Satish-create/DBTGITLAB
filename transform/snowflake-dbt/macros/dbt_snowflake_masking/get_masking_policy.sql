{%- macro get_masking_policy() -%}


{% set database = this.database  %}
{% set schema = this.schema  %}
{% set alias = this.identifier %} 

{%- set column_data_type_query -%}
SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  t.table_type,
  c.column_name,
  c.data_type
FROM "{{ database }}".information_schema.tables t
INNER JOIN "{{ database }}".information_schema.columns c
  ON c.table_schema = t.table_schema
  AND c.table_name = t.table_name
WHERE t.table_catalog =  '{{ database.upper() }}' --'PEMPEY_PROD'
  AND t.table_type IN ('BASE TABLE', 'VIEW')
  AND t.table_schema = '{{ schema.upper() }}' --'LEGACY'
  AND t.table_name = '{{ alias.upper() }}' --'TEST_TABLE'
ORDER BY t.table_schema,
  t.table_name;

{%- endset -%}

{% set column_policies = []  %}
{% set column_info = dict()  %}


{%- if execute -%}

  {%- for node in graph.nodes.values()
     | selectattr("resource_type", "equalto", "model")
     | selectattr("name", "equalto", alias.lower())
 
  -%}
  
     {# {% do log(node.columns, info=true) %} #}

    {%- for column in node.columns.values()
     | selectattr("meta")
    -%}
        {# {% do log(column.meta, info=true) %} #}

        {%- if column.meta['masking_policy'] -%}
        
          {# {% do log(column.name ~ ", has a masking policy of: " ~ column.meta['masking_policy'] ~ "_" ~ column.data_type, info=true) %} #}
          {% set column_info = ({"COLUMN_NAME" : column.name.upper() , "POLICY_NAME" : column.meta['masking_policy'].upper()  }) %}
          {% do column_policies.append(column_info) %}

        {%- endif -%}

    {%- endfor -%}
  
  {%- endfor -%}

  {# {% do log(column_policies, info=true) %} #}

  {% if column_policies %}

    {%- set result = run_query(column_data_type_query) %}

    {%- for policy in column_policies  -%}

      {%- for row in result.rows if row['COLUMN_NAME'] == policy['COLUMN_NAME'] -%} 

        {# {% do log("database: " ~ row['TABLE_CATALOG'] ~ " schema: " ~ row['TABLE_SCHEMA'] ~ " table_name: " ~ row['TABLE_NAME']  ~ " table_type: " ~ row['TABLE_TYPE'] ~ " column_name: " ~ row['COLUMN_NAME'] ~ " data_type: " ~ row['DATA_TYPE'] ~ " policy: " ~ policy['POLICY_NAME'], info=true) %} #}

        {{ create_masking_policy(row['TABLE_CATALOG'], row['TABLE_SCHEMA'], row['DATA_TYPE'], policy['POLICY_NAME']) }}

        {{ set_masking_policy(row['TABLE_CATALOG'], row['TABLE_SCHEMA'], row['TABLE_NAME'], row['TABLE_TYPE'], row['COLUMN_NAME'], row['DATA_TYPE'], policy['POLICY_NAME']) }}

      {%- endfor -%}    

    {%- endfor -%}

  {% endif %}

{%- endif -%}



{%- endmacro -%}