WITH source AS (

    SELECT *
    FROM {{ var("database") }}.salesforce_stitch.contact

), renamed AS(

    SELECT
        -- id
        id                          AS contact_id,
        name                        AS contact_name,
        email                       AS contact_email,

        -- keys
        accountid                   AS account_id,
        masterrecordid              AS master_record_id,
        ownerid                     AS owner_id,
        recordtypeid                AS record_type_id,
        reportstoid                 AS reports_to_id, 

        --contact info
        
        title                       AS contact_title,
        role__c                     AS contact_role,
        mobilephone                 AS mobile_phone,
        person_score__c             AS person_score,

        department                  AS department,
        contact_status__c           AS contact_status,
        requested_contact__c        AS requested_contact,
        inactive_contact__c         AS inactive_contact,
        hasoptedoutofemail          AS has_opted_out_email,
        invalid_email_address__c    AS invalid_email_address,
        isemailbounced              AS email_is_bounced,
        emailbounceddate            AS email_bounced_date,
        emailbouncedreason          AS email_bounced_reason,

        mailingstreet               AS mailing_address,
        mailingcity                 AS mailing_city,
        mailingstate                AS mailing_state,
        mailingstatecode            AS mailing_state_code,
        mailingcountry              AS mailing_country,
        mailingcountrycode          AS mailing_country_code,
        mailingpostalcode           AS mailing_zip_code,

        -- info
        using_ce__c                 AS using_ce,
        ee_trial_start_date__c      AS ee_trial_start_date,
        ee_trial_end_date__c        AS ee_trial_end_date,
        industry__c                 AS industry,
        responded_to_githost_price_change__c AS responded_to_githost_price_change, -- maybe we can exclude this if it's not relevant
        leadsource                  AS lead_source,
        lead_source_type__c         AS lead_source_type,
        outreach_stage__c           AS outreach_stage,
        account_type__c             AS account_type,
        mql_timestamp__c            AS marketo_qualified_lead_timestamp,
        mql_datetime__c             AS marketo_qualified_lead_datetime,
        mkto_si__last_interesting_moment__c 
                                    AS marketo_last_interesting_moment,
        mkto_si__last_interesting_moment_date__c 
                                    AS marketo_last_interesting_moment_date,


        --gl info
        account_owner__c            AS account_owner,
        ae_comments__c              AS ae_comments,
        business_development_rep__c AS business_development_rep_name,
        outbound_bdr__c             AS outbound_business_development_rep_name,

        -- metadata
        createdbyid                 AS created_by_id,
        createddate                 AS created_date,
        lastactivitydate            AS last_activity_date,
        lastcurequestdate           AS last_cu_request_date,
        lastcuupdatedate            AS last_cu_update_date,
        lastmodifiedbyid            AS last_modified_by_id,
        lastmodifieddate            AS last_modified_date,
        systemmodstamp


    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
