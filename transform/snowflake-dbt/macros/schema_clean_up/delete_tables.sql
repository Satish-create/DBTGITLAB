{#
Delete tables from a schema

Example Usage:  
dbt run-operation delete_tables --args "{schema: [dbt_pempey]}"
dbt run-operation delete_tables --args "{schema: [dbt_pempey], mode: 'outdated'}"
dbt run-operation delete_tables --args "{schema: [dbt_pempey], mode: 'include', tables: [stg_epicor_parts,stg_epicor_customer_groups]}"

Arguments:
    scheam: The list of schemas to operate on.
    mode: The operating mode of the operations. Availbe mode are as follows:
      incldue: All tables in provided like will be deleted.
      exclude: All tables not in provided list will be deleted.
      outdated: Tables that do not match the current dbt model witll be deleted.
      all: All tables in listed schemas will be deleted. This the defualt
    tables: The list of tables to be considerd for inclide and exclude operations. Default is none.

#}


{% macro delete_tables(schema, mode='all', tables=none) %} 
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a list') %}
  {% endif %}
{%- if execute -%}
  {% set valid_database = ['PROD','PREP']%}

  {% if mode=='outdated' %}
   

     '(' + 
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %} 
        '{{ node.name }}'{% if not loop.last %},{% endif %}
        {%- endfor %}
      + ')'
    

  {% endif %}


  {% call statement('get_tables', fetch_result=True) %}
  WITH prod_tables_and_views AS (
    SELECT
      table_schema,
      table_name,
      table_type
    FROM prod.information_schema.tables
    
  ),

  prep_tables_and_views AS (
    SELECT
      table_schema,
      table_name,
      table_type
    FROM prod.information_schema.tables

  ),

  unioned AS (

    SELECT *
    FROM prod_tables_and_views

    UNION 

    SELECT *
    FROM prep_tables_and_views
  ),

  table_list AS (
    SELECT *
    FROM unioned
    WHERE TRUE
    AND table_schema IN ({{ scheam }})
    --AND table_name IN ({{ table_list }})

  )

  /*
    select current.schema_name,
           current.ref_name,
           current.ref_type
    from (
      select schemaname as schema_name, 
             tablename  as ref_name, 
             'table'    as ref_type
      from pg_catalog.pg_tables pt 
      where schemaname in (
        {%- if schema is iterable and (var is not string and var is not mapping) -%}
          {%- for s in schema -%}
            '{{ s }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema }}'
        {%- endif -%}
      )
      union all
      select schemaname as schema_name, 
             viewname   as ref_name, 
             'view'     as ref_type
      from pg_catalog.pg_views
        where schemaname in (
        {%- if schema is iterable and (var is not string and var is not mapping) -%}
          {%- for s in schema -%}
            '{{ s }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema }}'
        {%- endif -%}
      )) as current     
      {% if mode=='include' %}
      where ref_name in (
        {%- for t in tables %} 
        '{{ t }}'{% if not loop.last %},{% endif %}
        {%- endfor %}
      )
      {% elif mode=='exclude' %}
      where ref_name not in (
        {%- for t in tables %} 
        '{{ t }}'{% if not loop.last %},{% endif %}
        {%- endfor %}
      )
      {% elif mode=='outdated' %}
      where ref_name not in (
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %} 
        '{{node.name}}'{% if not loop.last %},{% endif %}
        {%- endfor %}
      )
      {% elif mode=='all' %}
      {% endif %}
      */
  {% endcall %}

  {%- for to_delete in load_result('get_tables')['data'] %}         
    {%- set delete_relation = adapter.get_relation(
      database=target.dbname,
      schema=to_delete[0],
      identifier=to_delete[1]
    ) -%}
    {% if delete_relation %}
      {% do log('Dropping: ' ~ delete_relation, info=true) %}
      {# {% do adapter.drop_relation(delete_relation) %} #}
    {% endif %}    
  {%- endfor %}

{%- endif -%}

{% endmacro %}