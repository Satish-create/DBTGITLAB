WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ip_restrictions_source') }}

)

SELECT *
FROM source
