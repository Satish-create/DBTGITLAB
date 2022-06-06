{{ config(
    tags=["mnpi_exception", "pmg"]
) }}

WITH base AS (--NOTE: ONLY add columns to the END of the final query per PMG
    WITH tp AS(
        WITH bplc AS (
            WITH ca AS (

                SELECT
                    contact_id                            AS contact_id,
                    a.account_id                          AS account_id,
                    a.ultimate_parent_account_id          AS ultimate_parent_account_id,
                    a.ultimate_parent_account_name        AS ultimate_parent_account_name,
                    a.account_name                        AS account_name,
                    a.gtm_strategy                        AS gtm_strategy,
                    a.account_demographics_region         AS region,
                    a.sales_segment                       AS sales_segmentation,
                    inquiry_datetime                      AS inquiry_datetime,
                    marketo_qualified_lead_date           AS marketo_qualified_lead_date,
                    mql_datetime_inferred                 AS mql_datetime_inferred,
                    accepted_datetime                     AS accepted_datetime,
                    contact_status                        AS contact_status,
                    contact_title                         AS contact_title,
                    mailing_country                       AS mailing_country,
                    last_utm_campaign                     AS last_utm_campaign,
                    last_utm_content                      AS last_utm_content,
                    lead_source                           AS lead_source
            FROM {{ ref('sfdc_contact_xf') }} C
            LEFT JOIN {{ ref('sfdc_accounts_xf') }}  a ON C.account_id = a.account_id
)

    SELECT
      bp.person_id                                                AS person_id,
      bp.bizible_lead_id                                          AS bizible_lead_id,
      bp.bizible_contact_id                                       AS bizible_contact_id,
      l.inquiry_datetime                                          AS lead_inquiry_datetime,
      ca.inquiry_datetime                                         AS contact_inquiry_datetime,
      l.marketo_qualified_lead_date                               AS lead_mql_datetime,
      l.mql_datetime_inferred                                     AS lead_mql_datetime_inferred,
      ca.marketo_qualified_lead_date                              AS contact_mql_datetime,
      ca.mql_datetime_inferred                                    AS contact_mql_datetime_inferred,
      l.accepted_datetime                                         AS lead_accepted_datetime,
      ca.accepted_datetime                                        AS contact_accepted_datetime,
      l.lead_status                                               AS lead_status,
      ca.contact_status                                           AS contact_status,
      l.title                                                     AS lead_title,
      ca.contact_title                                            AS contact_title,
      l.account_demographics_region                               AS region,
      ca.region                                                   AS account_region,
      l.sales_segmentation                                        AS sales_segmentation,
      ca.sales_segmentation                                       AS account_sales_segmentation,
      l.country                                                   AS country,
      ca.mailing_country                                          AS mailing_country,
      l.company                                                   AS company,
      ca.account_name                                             AS account_name,
      ca.gtm_strategy                                             AS gtm_strategy,
      ca.account_id                                               AS account_id,
      ca.ultimate_parent_account_id                               AS ultimate_parent_account_id,
      ca.ultimate_parent_account_name                             AS ultimate_parent_account_name,
      l.last_utm_campaign                                         AS last_utm_campaign,
      l.last_utm_content                                          AS last_utm_content,
      ca.last_utm_campaign                                        AS last_utm_campaign,
      ca.last_utm_content                                         AS last_utm_content,
      l.lead_source                                               AS lead_source,
      ca.lead_source                                              AS lead_source,
      CASE
      WHEN l.lead_source IS NOT NULL THEN l.lead_source
      ELSE ca.lead_source END AS person_lead_source,
      CASE
      WHEN bp.bizible_contact_id  IS NOT NULL THEN bp.bizible_contact_id
      ELSE bp.bizible_lead_id END AS sfdc_person_id,
      CASE
      WHEN ca.region IS NOT NULL THEN ca.region
      ELSE l.account_demographics_region
      END AS person_region,
      CASE
      WHEN ca.sales_segmentation IS NOT NULL THEN ca.sales_segmentation
      ELSE l.sales_segmentation
      END AS person_sales_segmentation,
      CASE
      WHEN ca.mailing_country IS NOT NULL THEN ca.mailing_country
      ELSE l.country
      END AS person_country,
      CASE
      WHEN ca.contact_status IS NOT NULL THEN ca.contact_status
      ELSE l.lead_status
      END AS person_status,
      CASE
      WHEN ca.contact_title IS NOT NULL THEN ca.contact_title
      ELSE l.title
      END AS person_title,
      CASE
      WHEN ca.account_name IS NOT NULL THEN ca.account_name
      ELSE l.company
      END AS person_company,
      CASE
      WHEN ca.last_utm_campaign IS NOT NULL THEN ca.last_utm_campaign
      ELSE l.last_utm_campaign
      END AS person_last_utm_campaign,
      CASE
      WHEN ca.last_utm_content IS NOT NULL THEN ca.last_utm_content
      ELSE l.last_utm_content
      END AS person_last_utm_content,
      CASE
      WHEN ca.inquiry_datetime IS NOT NULL THEN ca.inquiry_datetime::DATE
      ELSE l.inquiry_datetime::DATE
      END AS person_inquiry_datetime,
      CASE
      WHEN ca.marketo_qualified_lead_date IS NOT NULL THEN ca.marketo_qualified_lead_date::DATE
      ELSE l.marketo_qualified_lead_date::DATE
      END AS person_mql_datetime,
      CASE
      WHEN ca.mql_datetime_inferred IS NOT NULL THEN ca.mql_datetime_inferred::DATE
      ELSE l.mql_datetime_inferred::DATE
      END AS person_mql_datetime_inferred,
      CASE
      WHEN ca.accepted_datetime IS NOT NULL THEN ca.accepted_datetime::DATE
      ELSE l.accepted_datetime::DATE
      END AS person_accepted_datetime

    FROM {{ ref('sfdc_bizible_person') }} bp
    LEFT JOIN {{ ref('sfdc_lead_xf') }} l ON bp.bizible_lead_id = l.lead_id
    LEFT JOIN ca ON bp.bizible_contact_id = ca.contact_id
    WHERE bp.is_deleted = FALSE

), campaign AS (

    SELECT *
    FROM {{ ref('sfdc_campaign') }}

)

SELECT
date_trunc('month',tp.bizible_touchpoint_date)::DATE::DATE  AS bizible_touchpoint_date_month_yr,
tp.bizible_touchpoint_date::DATE                            AS bizible_touchpoint_date_normalized,
tp.bizible_touchpoint_date,
tp.touchpoint_id,
tp.bizible_touchpoint_type,
camp.type AS campaign_type,
tp.bizible_touchpoint_source,
tp.bizible_medium,
1 AS Touchpoint_count,
tp.bizible_person_id,
bplc.sfdc_person_id,
tp.bizible_count_lead_creation_touch,
CASE
WHEN camp.campaign_parent_id = '7014M000001dn8MQAQ'
   THEN 'Paid Social.LinkedIn Lead Gen'
WHEN bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI'
   THEN 'Sponsorship'
ELSE tp.bizible_marketing_channel_path
END AS bizible_marketing_channel_path,
tp.bizible_landing_page,
tp.bizible_form_url,
tp.bizible_referrer_page,
tp.bizible_ad_campaign_name,
tp.bizible_ad_content,
tp.bizible_form_url_raw,
tp.bizible_landing_page_raw,
tp.bizible_referrer_page_raw,
bplc.person_inquiry_datetime:: DATE                     AS person_inquiry_datetime,
bplc.person_mql_datetime:: DATE                         AS person_mql_datetime,
bplc.person_accepted_datetime:: DATE                    AS person_accepted_datetime,
bplc.person_status,
bplc.person_lead_source ,
bplc.person_last_utm_campaign,
bplc.person_last_utm_content,
CASE
  WHEN bplc.person_region = 'NORAM' THEN 'AMER'
  ELSE bplc.person_region
  END AS person_region,
IFF(bplc.person_sales_segmentation IS NULL,'Unknown',bplc.person_sales_segmentation) AS sfdc_sales_segmentation,
bplc.person_company,
bplc.account_id,
bplc.account_name,
bplc.ultimate_parent_account_id,
bplc.ultimate_parent_account_name,
bplc.gtm_strategy,
UPPER(bplc.person_title)        AS person_title,
UPPER(bplc.country)             AS person_country,
CASE
 WHEN bplc.person_mql_datetime >= bizible_touchpoint_date_normalized THEN '1'
 ELSE '0'
 END AS count_mql,
CASE
 WHEN count_mql=1 THEN bplc.sfdc_person_id
 ELSE NULL
 END AS mql_person,
CASE
 WHEN bplc.person_mql_datetime >= bizible_touchpoint_date_normalized THEN tp.bizible_count_lead_creation_touch
 ELSE '0'
 END AS count_net_new_mql,
CASE
 WHEN bplc.person_accepted_datetime >= bizible_touchpoint_date_normalized THEN '1'
 ELSE '0'
 END AS count_accepted,
CASE
 WHEN bplc.person_accepted_datetime >= bizible_touchpoint_date_normalized THEN tp.bizible_count_lead_creation_touch
 ELSE '0'
 END AS count_net_new_accepted,
{{ gtm_motion() }} as gtm_motion
FROM {{ ref('sfdc_bizible_touchpoint') }} tp
LEFT JOIN bplc ON bplc.person_id = tp.bizible_person_id
LEFT JOIN campaign camp ON tp.campaign_id=camp.campaign_id
WHERE tp.is_deleted = FALSE

), ol AS (

    WITH biz_base AS (
        WITH oa AS(
            SELECT
              o.opportunity_id                AS opportunity_id,
              o.created_date                  AS opp_created_date,
              o.sales_accepted_date           AS sales_accepted_date,
              o.close_date                    AS close_date,
              o.deal_path                     AS deal_path,
              o.stage_name                    AS stage_name,
              o.order_type_stamped            AS order_type_stamped,
              o.is_won                        AS is_won,
              o.incremental_acv               AS iacv,
              a.sales_segment                 AS sales_segment,
              a.account_demographics_region   AS account_region,
              a.account_id                    AS account_id,
              a.account_name                  AS account_name,
              a.gtm_strategy                  AS gtm_strategy
    FROM {{ ref('sfdc_opportunity_xf') }}  o
    LEFT JOIN {{ ref('sfdc_accounts_xf') }} a ON o.account_id = a.account_id
)

SELECT
  bat.*,
  opp_created_date                                        AS opp_created_date,
  sales_accepted_date                                     AS sales_accepted_date,
  close_date                                              AS close_date,
  deal_path                                               AS deal_path,
  stage_name                                              AS stage_name,
  is_won                                                  AS is_won,
  iacv                                                    AS iacv,
  IFF(sales_segment IS NULL,'Unknown',sales_segment)      AS sales_segment,
  account_region                                          AS account_region,
  account_id                                              AS account_id,
  account_name                                            AS account_name,
  gtm_strategy                                            AS gtm_strategy,
  order_type_stamped                                      AS order_type_stamped
FROM  {{ ref('sfdc_bizible_attribution_touchpoint_xf') }}  bat
LEFT JOIN oa opp ON bat.opportunity_id = opp.opportunity_id
WHERE stage_name NOT LIKE '%Duplicate%'
AND LOWER(is_deleted) LIKE 'false'

), contacts AS (

SELECT
    contact_id          AS contact_id,
    mailing_country     AS mailing_country,
    last_utm_campaign   AS person_last_utm_campaign,
    last_utm_content    AS person_last_utm_content
FROM {{ ref('sfdc_contact_xf') }}

), campaign AS (

SELECT *
FROM {{ ref('sfdc_campaign') }}

), linear_base AS (

SELECT
    opportunity_id,
    iacv,
    COUNT(DISTINCT biz_base.touchpoint_id)  AS count_touches,
    iacv/count_touches                      AS weighted_linear_iacv
FROM biz_base
GROUP BY 1,2

), campaigns_per_opp AS (

SELECT
    opportunity_id,
    COUNT(DISTINCT biz_base.campaign_id) AS campaigns_per_opp
FROM biz_base
GROUP BY 1

)

SELECT
    biz_base.opportunity_id AS opportunity_id,
    touchpoint_id,
    biz_base.campaign_id,
    biz_base.bizible_contact,
    contacts.mailing_country AS country ,
    biz_base.iacv,
    biz_base.account_id,
    biz_base.account_name,
    biz_base.gtm_strategy,
    (biz_base.iacv / campaigns_per_opp.campaigns_per_opp) AS iacv_per_campaign ,
    count_touches,
    bizible_touchpoint_date,
    bizible_touchpoint_position,
    bizible_touchpoint_source,
    bizible_touchpoint_type ,
    bizible_ad_campaign_name,
    bizible_ad_content,
    bizible_form_url_raw,
    bizible_landing_page_raw,
    bizible_referrer_page_RAW ,
    bizible_form_url,
    bizible_landing_page,
    bizible_marketing_channel,
    CASE WHEN camp.campaign_parent_id = '7014M000001dn8MQAQ'
      THEN 'Paid Social.LinkedIn Lead Gen'
    WHEN bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI'
      THEN 'Sponsorship'
    ELSE bizible_marketing_channel_path
    END AS marketing_channel_path,
    pipe_name,
    bizible_medium,
    bizible_referrer_page,
    biz_base.lead_source AS lead_source,
    opp_created_date::DATE AS opp_created_date,
    sales_accepted_date::DATE AS sales_accepted_date,
    close_date::DATE AS close_date,
    sales_qualified_month,
    biz_base.sales_type,
    biz_base.stage_name,
    biz_base.is_won,
    biz_base.deal_path,
    biz_base.order_type_stamped,
    IFF(biz_base.sales_segment IS NULL,'Unknown',biz_base.sales_segment) AS sales_segment,
    biz_base.account_region,
    date_trunc('month',bizible_touchpoint_date)::DATE::DATE AS bizible_touchpoint_date_month_yr  ,
    bizible_touchpoint_date::DATE AS bizible_touchpoint_date_normalized,
    camp.type AS campaign_type,
    contacts.person_last_utm_campaign,
    contacts.person_last_utm_content,
    {{ gtm_motion() }} as gtm_motion,
    SUM(bizible_count_first_touch)                  AS first_weight,
    SUM(bizible_count_w_shaped)                     AS w_weight,
    SUM(bizible_count_u_shaped)                     AS u_weight,
    SUM(bizible_attribution_percent_full_path)      AS full_weight,
    COUNT(DISTINCT biz_base.opportunity_id)         AS l_touches,
    (l_touches / count_touches)                     AS l_weight,
    (biz_base.iacv * first_weight)                  AS first_iacv,
    (biz_base.iacv * w_weight)                      AS w_iacv,
    (biz_base.iacv * u_weight)                      AS u_iacv,
    (biz_base.iacv * full_weight)                   AS full_iacv,
    (biz_base.iacv* l_weight)                       AS linear_iacv
FROM biz_base
LEFT JOIN linear_base ON biz_base.opportunity_id = linear_base.opportunity_id
LEFT JOIN campaigns_per_opp ON biz_base.opportunity_id = campaigns_per_opp.opportunity_id
LEFT JOIN campaign camp ON biz_base.campaign_id=camp.campaign_id
LEFT JOIN contacts ON biz_base.bizible_contact=contacts.contact_id
{{ dbt_utils.group_by(n=47) }}


), unioned AS (

SELECT
  tp.bizible_touchpoint_date_normalized         AS bizible_touchpoint_date_normalized,
  tp.bizible_integrated_campaign_grouping       AS bizible_integrated_campaign_grouping,
  tp.bizible_marketing_channel_path             AS bizible_marketing_channel_path,
  tp.bizible_form_url_raw                       AS bizible_form_url_raw,
  tp.bizible_landing_page_raw                   AS bizible_landing_page_raw,
  tp.sfdc_sales_segmentation                    AS sales_segment,
  NULL                                          AS sales_type,
  NULL                                          AS sales_qualified_month,
  NULL                                          AS sales_accepted_date,
  NULL                                          AS stage_name,
  tp.bizible_medium                             AS bizible_medium,
  tp.bizible_touchpoint_source                  AS bizible_touchpoint_source,
  tp.bizible_ad_campaign_name                   AS bizible_ad_campaign_name,
  tp.bizible_ad_content                         AS bizible_ad_content,
  tp.person_last_utm_campaign                   AS person_last_utm_campaign,
  tp.person_last_utm_content                    AS person_last_utm_content,
  0                                             AS total_cost,
  SUM (tp.touchpoint_count)                     AS touchpoint_sum,
  SUM (tp.count_mql)                            AS mql_sum,
  SUM (tp.count_accepted)                       AS accepted_sum,
  0                                             AS linear_sao
FROM tp
WHERE tp.bizible_integrated_campaign_grouping <> 'None'
{{ dbt_utils.group_by(n=16) }}
UNION ALL
SELECT
  ol.bizible_touchpoint_date_normalized         AS opp_touchpoint_date_normalized,
  ol.bizible_integrated_campaign_grouping       AS opp_integrated_campaign_grouping,
  ol.marketing_channel_path                     AS marketing_channel_path,
  ol.bizible_form_url_raw                       AS bizible_form_url_raw,
  ol.bizible_landing_page_raw                   AS bizible_landing_page_raw,
  ol.sales_segment                              AS sales_segment,
  ol.sales_type                                 AS sales_type,
  ol.sales_qualified_month                      AS sales_qualified_month,
  ol.sales_accepted_date                        AS sales_accepted_date,
  ol.stage_name                                 AS stage_name,
  ol.bizible_medium                             AS bizible_medium,
  ol.bizible_touchpoint_source                  AS bizible_touchpoint_source,
  ol.bizible_ad_campaign_name                   AS bizible_ad_campaign_name,
  ol.bizible_ad_content                         AS bizible_ad_content,
  ol.person_last_utm_campaign                   AS person_last_utm_campaign,
  ol.person_last_utm_content                    AS person_last_utm_content,
  0                                             AS total_cost,
  0                                             AS touchpoint_sum,
  0                                             AS mql_sum,
  0                                             AS accepted_sum,
  CASE
  WHEN ol.sales_accepted_date IS NOT NULL THEN SUM(ol.L_WEIGHT)
  ELSE 0 END AS linear_sao
FROM ol
WHERE ol.bizible_integrated_campaign_grouping <> 'None'
{{ dbt_utils.group_by(n=16) }}

), final AS (

    SELECT
      bizible_touchpoint_date_normalized                   AS sao_date,
      bizible_integrated_campaign_grouping                 AS bizible_integrated_campaign_grouping,
      sales_segment                                        AS sdfc_sales_segment,
      bizible_ad_campaign_name                             AS bizible_ad_campaign_name,
      bizible_marketing_channel_path                       AS bizible_marketing_channel_path,
      bizible_medium                                       AS bizible_medium,
      bizible_touchpoint_source                            AS bizible_source,
      CASE
        WHEN SPLIT_PART(SPLIT_PART(bizible_form_url_raw,'utm_campaign=',2),'&',1)= ''
        THEN SPLIT_PART(SPLIT_PART(bizible_landing_page_raw,'utm_campaign=',2),'&',1)
      ELSE SPLIT_PART(SPLIT_PART(bizible_form_url_raw,'utm_campaign=',2),'&',1) END AS utm_campaign,
      SPLIT_PART(utm_campaign,'_',1)                        AS utm_campaigncode,
      SPLIT_PART(utm_campaign,'_',2)                        AS utm_geo,
      SPLIT_PART(utm_campaign,'_',3)                        AS utm_targeting,
      SPLIT_PART(utm_campaign,'_',4)                        AS utm_ad_unit,
      SPLIT_PART(utm_campaign,'_',5)                        AS "utm_br/bn",
      SPLIT_PART(utm_campaign,'_',6)                        AS utm_matchtype,
      CASE
        WHEN SPLIT_PART(SPLIT_PART(bizible_form_url_raw,'utm_content=',2),'&',1)= ''
        THEN SPLIT_PART(SPLIT_PART(bizible_landing_page_raw,'utm_content=',2),'&',1)
      ELSE SPLIT_PART(SPLIT_PART(bizible_form_url_raw,'utm_content=',2),'&',1) END AS utm_content,
      SPLIT_PART(utm_content,'_',1)                         AS utm_contentcode,
      SPLIT_PART(utm_content,'_',2)                         AS utm_team,
      SPLIT_PART(utm_content,'_',3)                         AS utm_segment,
      SPLIT_PART(utm_content,'_',4)                         AS utm_language,
      person_last_utm_campaign                              AS person_last_utm_campaign,
      person_last_utm_content                               AS person_last_utm_content,
      bizible_form_url_raw                                  AS bizible_form_url_raw,
      bizible_landing_page_raw                              AS bizible_landing_page_raw,
      SUM(touchpoint_sum)                                   AS inquiries,
      SUM(mql_sum)                                          AS mqls,
      SUM(accepted_sum)                                     AS sdr_accepted,
      SUM(linear_sao)                                       AS linear_sao
  FROM unioned
  WHERE sao_date >= '2020-02-01'::DATE
  AND bizible_integrated_campaign_grouping <>'None'
  {{ dbt_utils.group_by(n=23) }}

)

SELECT *
FROM final
ORDER BY 1 DESC

)

SELECT *
FROM base
ORDER BY sao_date DESC
