WITH source AS (

	SELECT *
	FROM {{ var("database") }}.salesforce_stitch.campaignmember

), renamed AS(

	SELECT
      id              AS campaign_member_id,
     
	 	--keys
      campaignid      AS campaign_id,
      leadorcontactid AS lead_or_contact_id,

		--info
      type            AS campaign_member_type,

		--data_quality_description__c as data_quality_description,
		--data_quality_score__c as data_quality_score,
		--projections
		--results
		--metadata
      createddate    AS campaign_member_created_date,
	  systemmodstamp

	FROM source
	WHERE isdeleted = FALSE

)

SELECT *
FROM renamed


--- excluded columns
