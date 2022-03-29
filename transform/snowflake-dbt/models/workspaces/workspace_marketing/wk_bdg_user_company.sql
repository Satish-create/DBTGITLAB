{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('users','gitlab_dotcom_users_source'),
    ('sf_leads','sfdc_lead_source'),
    ('sf_contacts','sfdc_contact_source')
]) }},

users_enhance AS (

  SELECT
    *
  FROM {{ ref('gitlab_contact_enhance_source') }}
  WHERE zoominfo_company_id != '0'
    AND zoominfo_company_id != ''

),

rpt AS (

  SELECT
    users.user_id AS gitlab_dotcom_user_id,
    COALESCE(
      sf_leads.zoominfo_company_id,
      sf_contacts.zoominfo_company_id,
      users_enhance.zoominfo_company_id
    ) AS company_id,
    sf_leads.zoominfo_company_id AS sf_lead_company_id,
    sf_contacts.zoominfo_company_id AS sf_contact_company_id,
    users_enhance.zoominfo_company_id AS enhance_company_id,
    {{ dbt_utils.surrogate_key(['users.user_id']) }} AS dim_user_id,
    {{ dbt_utils.surrogate_key(['company_id']) }} AS dim_company_id
  FROM users
  LEFT JOIN sf_leads
    ON users.email = sf_leads.lead_email
  LEFT JOIN sf_contacts
    ON users.email = sf_contacts.contact_email
  LEFT JOIN users_enhance
    ON users.user_id = users_enhance.row_integer
  WHERE company_id IS NOT NULL

)

SELECT * FROM rpt
