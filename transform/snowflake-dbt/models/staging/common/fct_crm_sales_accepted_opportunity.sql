WITH sfdc_opportunity AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity')}}

), first_contact_roles  AS (
  
  SELECT
  
    created_date,
    opportunity_contact_role_id,
    opportunity_id,
    contact_id,
    ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC) AS row_num

  FROM {{ ref('sfdc_opportunity_contact_role')}}

), sales_accepted_opps AS (

  SELECT
  
    {{ dbt_utils.surrogate_key(['sfdc_opportunity.opportunity_id','sales_accepted_date']) }} AS event_id,
    sfdc_opportunity.sales_accepted_date                                                     AS sales_accepted_date,
    {{ dbt_utils.surrogate_key(['first_contact_roles.contact_id']) }}                        AS crm_person_id,
    first_contact_roles.contact_id                                                           AS contact_id,
    sfdc_opportunity.account_id                                                              AS account_id,
    sfdc_opportunity.opportunity_id                                                          AS opportunity_id,
    first_contact_roles.opportunity_contact_role_id                                          AS opportunity_contact_role_id

  FROM sfdc_opportunity
  INNER JOIN first_contact_roles 
    ON sfdc_opportunity.opportunity_id = first_contact_roles.opportunity_id 
	  AND row_num = 1
  WHERE sfdc_opportunity.sales_accepted_date IS NOT NULL
    AND is_edu_oss = 0
	AND stage_name != '10-Duplicate'
    AND sales_qualified_source IN (
                                  'SDR Generated'
                                  , 'BDR Generated'
                                  )
)

SELECT *
FROM sales_accepted_opps