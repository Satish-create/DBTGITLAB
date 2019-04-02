with source as (

  SELECT * 
  FROM {{ var("database") }}.tap_postgres.version_db_version_checks

), renamed as (

  SELECT  id,
          host_id,

          created_at,
          updated_at,

          gitlab_version,
          referer_url,
          request_data
  FROM source
)

SELECT * 
FROM renamed
WHERE created_at >= '2019-01-20'
