WITH source AS (

    SELECT *
    FROM {{ var("database") }}.salesforce_stitch.statement_of_work__c

), renamed AS (

    SELECT
    -- keys
      id                                 AS statement_of_work_id,
      opportunity__c                     AS opportunity_id,
      owner__c                           AS owner_id,

    -- Dates
      completed_date__c                  AS completed_date,
      kick_off_date__c                   AS kick_off_date,
      go_live_date__c                    AS go_live_date,

    -- Info
      name                               AS statement_of_work_name,
      percentcomplete__c                 AS percent_complete,
      signed_acceptance_from_customer__c AS signed_acceptance_from_customer,
      status__c                          AS status,

    -- metadata
      createdbyid                        AS created_by_id,
      lastmodifiedbyid                   AS last_modified_by_id,
      createddate                        AS created_date,
      lastmodifieddate                   AS last_modified_date

    FROM source

)

SELECT *
FROM renamed