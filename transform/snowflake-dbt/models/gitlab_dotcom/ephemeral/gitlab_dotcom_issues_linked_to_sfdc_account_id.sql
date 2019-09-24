{{ config({
    "materialized": "ephemeral"
    })
}}

WITH gitlab_issues AS (

  SELECT 
   issue_id,
   {{target.schema}}_staging.regexp_to_array(issue_description, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array
   
  FROM {{ ref('gitlab_dotcom_issues_xf')}}
  WHERE is_internal_issue
    AND issue_description IS NOT NULL

)

, sfdc_accounts AS (

  SELECT *
  FROM {{ ref('sfdc_accounts_xf')}}

)

, sfdc_contacts AS (

  SELECT *
  FROM {{ ref('sfdc_contact_xf')}}

)

, sfdc_leads AS (

  SELECT *
  FROM {{ ref('sfdc_lead_xf')}}

)

, sfdc_opportunities AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_xf')}}

)

, gitlab_issues_sfdc_id_flattened AS (

  SELECT
    issue_id,
    {{target.schema}}_staging.id15to18(CAST(f.value AS VARCHAR)) AS sfdc_id_18char

  FROM gitlab_issues, table(flatten(sfdc_link_array)) f
)

, gitlab_issues_with_sfdc_accounts AS (

  SELECT
    gitlab_issues_sfdc_id_flattened.issue_id,
    sfdc_accounts.account_id AS sfdc_account_id

  FROM gitlab_issues_sfdc_id_flattened
  INNER JOIN sfdc_accounts
    ON gitlab_issues_sfdc_id_flattened.sfdc_id_18char = sfdc_accounts.account_id

)
, gitlab_issues_with_sfdc_opportunities AS (

  SELECT
    gitlab_issues_sfdc_id_flattened.issue_id,
    sfdc_opportunities.account_id AS sfdc_account_id

  FROM gitlab_issues_sfdc_id_flattened
  INNER JOIN sfdc_opportunities
    ON gitlab_issues_sfdc_id_flattened.sfdc_id_18char = sfdc_opportunities.opportunity_id

)
, gitlab_issues_with_sfdc_leads AS (

  SELECT
    gitlab_issues_sfdc_id_flattened.issue_id,
    sfdc_leads.converted_account_id AS sfdc_account_id

  FROM gitlab_issues_sfdc_id_flattened
  INNER JOIN sfdc_leads
    ON gitlab_issues_sfdc_id_flattened.sfdc_id_18char = sfdc_leads.lead_id

)
, gitlab_issues_with_sfdc_contacts AS (

  SELECT
    gitlab_issues_sfdc_id_flattened.issue_id,
    sfdc_contacts.account_id AS sfdc_account_id

  FROM gitlab_issues_sfdc_id_flattened
  INNER JOIN {{ ref('sfdc_contact_xf')}} AS sfdc_contacts
    ON gitlab_issues_sfdc_id_flattened.sfdc_id_18char = sfdc_contacts.contact_id
)
, gitlab_issues_with_sfdc_objects_union AS (

  SELECT
    *
  FROM gitlab_issues_with_sfdc_accounts

  UNION

  SELECT
    *
  FROM gitlab_issues_with_sfdc_opportunities

  UNION

  SELECT
    *
  FROM gitlab_issues_with_sfdc_contacts

)

SELECT *
FROM gitlab_issues_with_sfdc_objects_union
