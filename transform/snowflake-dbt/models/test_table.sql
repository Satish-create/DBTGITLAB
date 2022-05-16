{{ config({ "post-hook": '{{ mask_model() }}'}) }}

select
  'A'::VARCHAR AS the_varchar,
  0.01::FLOAT AS the_float,
  1::INTEGER AS the_int,
  to_array('A')::ARRAY AS the_array,
  parse_json('{"A":"a","B":"b"}')::VARIANT AS the_variant,
  '2022-04-18'::DATE AS the_date,
  '2022-04-18 01:01:01'::TIMESTAMP AS the_timestamp,
  TRUE::BOOLEAN AS the_boolean

/*  {{ mask_model() }}
 {{ source('test_source','test_source_table') }}

*/