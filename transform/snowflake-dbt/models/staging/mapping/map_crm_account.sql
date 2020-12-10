{{ config({
        "schema": "common_mapping"
    })
}}

WITH account_prep AS (

    SELECT *
    FROM {{ ref('prep_sfdc_account') }}

), geo_region AS (

    SELECT *
    FROM {{ ref('dim_geo_region') }}

), geo_sub_region AS (

    SELECT *
    FROM {{ ref('dim_geo_sub_region') }}

), geo_area AS (

    SELECT *
    FROM {{ ref('dim_geo_area') }}

), sales_segment AS (

    SELECT *
    FROM {{ ref('dim_sales_segment') }}

), sales_territory AS (

    SELECT *
    FROM {{ ref('dim_sales_territory') }}

), industry AS (

    SELECT *
    FROM {{ ref('dim_industry') }}

), final AS (
    
    SELECT

      account_prep.crm_account_id          AS crm_account_id,
      dim_sales_segment_id,
      dim_geo_region_id,
      dim_geo_sub_region_id,
      dim_geo_area_id,
      dim_sales_territory_id,
      dim_industry_id

    FROM account_prep 
    LEFT JOIN geo_region         
      ON account_prep.geo_region_name = geo_region.geo_region_name
    LEFT JOIN geo_sub_region
      ON account_prep.geo_sub_region_name = geo_sub_region.geo_sub_region_name
    LEFT JOIN geo_area      
      ON account_prep.geo_area_name = geo_area.geo_area_name
    LEFT JOIN sales_segment
      ON account_prep.sales_segment_name = sales_segment.sales_segment_name
    LEFT JOIN sales_territory
      ON account_prep.sales_territory_name = sales_territory.sales_territory_name
    LEFT JOIN industry      
      ON account_prep.industry_name = industry.industry_name
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@paul_armstrong",
    created_date="2020-11-23",
    updated_date="2020-12-10"
) }}