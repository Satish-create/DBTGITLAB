{{
  config({
    "materialized":"table",
    "post-hook": [
       "CREATE INDEX idx_f_opportunity_stageid ON {{ this.schema }}.f_opportunity(opportunity_stage_id)",
       "CREATE INDEX idx_f_opportunity_closedate ON {{ this.schema }}.f_opportunity(opportunity_closedate)",
       "CREATE INDEX idx_f_opportunity_leadource ON {{ this.schema }}.f_opportunity(lead_source_id)",
       "CREATE INDEX idx_f_opportunity_account_id ON {{ this.schema }}.f_opportunity(account_id)",
       "ALTER TABLE {{ this.schema }}.dim_opportunitystage ADD PRIMARY KEY(id)",
       "ALTER TABLE {{ this.schema }}.dim_leadsource ADD PRIMARY KEY(id)",
       "ALTER TABLE {{ this.schema }}.dim_account ADD PRIMARY KEY(id)",
       "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase"
    ]
  })
}}


with lineitems as (
		select * from {{ ref('lineitems') }}

),
oppstage as (
		select * from {{ ref('dim_opportunitystage') }}

),

opportunity as (
	select * from {{ ref('opportunity') }}

),

leadsource as (
       select * from {{ ref('dim_leadsource') }}
),

account as (
       select * from {{ ref('dim_account') }}
)

SELECT o.sfdc_id AS opportunity_id
       , a.id AS account_id
       , s.id AS opportunity_stage_id
       , l.id AS lead_source_id
       , COALESCE(o.type, 'Unknown') AS opportunity_type
       , COALESCE(o.sales_segmentation_o__c, 'Unknown') as opportunity_sales_segmentation
       , o.sales_qualified_date__c as sales_qualified_date
       , o.sales_accepted_date__c as sales_accepted_date
       , o.sql_source__c as sales_qualified_source
       , o.closedate AS opportunity_closedate
       , COALESCE(i.product, 'Unknown') as opportunity_product
       , COALESCE(i.period, 'Unknown') as billing_period
       , COALESCE(o.name, 'Unknown') as opportunity_name
       , i.qty as quantity
       , i.iacv
       , i.renewal_acv
       , i.acv
       , i.tcv
FROM lineitems i
INNER JOIN opportunity o ON i.opportunity_id=o.sfdc_id
INNER JOIN oppstage s ON o.stagename=s.masterlabel
INNER JOIN leadsource l on o.leadsource=l.Initial_Source
INNER JOIN account a on o.accountId=a.sfdc_account_id