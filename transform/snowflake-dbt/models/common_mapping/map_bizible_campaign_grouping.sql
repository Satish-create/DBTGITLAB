WITH bizible AS (
 
    SELECT *
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'


), campaign AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), touchpoints_with_campaign AS (
    
    SELECT 
      {{ dbt_utils.surrogate_key(['campaign.dim_campaign_id','campaign.dim_parent_campaign_id',
      								'bizible.bizible_touchpoint_type','bizible.bizible_landing_page',
      								'bizible.bizible_referrer_page','bizible.bizible_form_url',
      								'bizible.bizible_ad_campaign_name','bizible.bizible_marketing_channel_path'
      							]) 
      }}																										AS bizible_campaign_grouping_id,
      campaign.dim_campaign_id,
      campaign.dim_parent_campaign_id,
      bizible.bizible_touchpoint_type,
      bizible.bizible_landing_page,
      bizible.bizible_referrer_page,
      bizible.bizible_form_url,
      bizible.bizible_ad_campaign_name,
      bizible.bizible_marketing_channel_path,
       CASE
   When camp.campaign_parent_id = '7014M000001dowZQAQ' -- based on issue https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/246
    OR (bizible_medium = 'sponsorship'
      AND bizible_touchpoint_source IN ('issa','stackoverflow','securityweekly-appsec'))
    THEN 'Publishers/Sponsorships' 
  When  (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%smbnurture%' 
    OR bizible_form_url LIKE '%smbnurture%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%smbnurture%'
    OR bizible_ad_campaign_name LIKE '%smbnurture%'
    OR bizible_landing_page LIKE '%smbagnostic%' 
    OR bizible_form_url LIKE '%smbagnostic%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%smbagnostic%'
    OR bizible_ad_campaign_name LIKE '%smbagnostic%'))
    OR bizible_ad_campaign_name = 'Nurture - SMB Mixed Use Case'
    THEN 'SMB Nurture' 
  When  (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%cicdseeingisbelieving%' 
    OR bizible_form_url LIKE '%cicdseeingisbelieving%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%cicdseeingisbelieving%'
    OR bizible_ad_campaign_name LIKE '%cicdseeingisbelieving%'))
    OR camp.campaign_parent_id = '7014M000001dmNAQAY'
    THEN 'CI/CD Seeing is Believing' 
  When  (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%simplifydevops%' 
    OR bizible_form_url LIKE '%simplifydevops%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%simplifydevops%'
    OR bizible_ad_campaign_name LIKE '%simplifydevops%'))
    OR camp.campaign_parent_id = '7014M000001doAGQAY'
    OR camp.campaign_id LIKE '7014M000001dn6z%'
    THEN 'Simplify DevOps' 
  When  (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%21q4-jp%' 
    OR bizible_form_url LIKE '%21q4-jp%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%21q4-jp%'
    OR bizible_ad_campaign_name LIKE '%21q4-jp%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ='2021_Social_Japan_LinkedIn Lead Gen')
    THEN 'Japan-Digital Readiness' 
  When  (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%lower-tco%' 
    OR bizible_form_url LIKE '%lower-tco%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%lower-tco%'
    OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'
    OR bizible_ad_campaign_name LIKE '%operationalefficiences%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Operational Efficiencies%'
        OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'))
    THEN 'Increase Operational Efficiencies' 
  When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%reduce-cycle-time%' 
    OR bizible_form_url LIKE '%reduce-cycle-time%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%reduce-cycle-time%'
    OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Better Products Faster%'
        OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
    THEN 'Deliver Better Products Faster'
  When (bizible_touchpoint_type = 'Web Form'
    AND (bizible_landing_page LIKE '%secure-apps%' 
    OR bizible_form_url LIKE '%secure-apps%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%secure-apps%'
    OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_Reduce Security Risk%'
        OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
    THEN 'Reduce Security and Compliance Risk'
  When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%jenkins-alternative%' 
    OR bizible_form_url LIKE '%jenkins-alternative%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%jenkins-alternative%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ' 
    AND (bizible_ad_campaign_name LIKE '%_Jenkins%'
        OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
    THEN 'Jenkins Take Out'
  When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%single-application-ci%' 
    OR bizible_form_url LIKE '%single-application-ci%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%single-application-ci%'
    OR bizible_ad_campaign_name LIKE '%cicdcmp3%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ' 
    AND bizible_ad_campaign_name LIKE '%cicdcmp3%')
    THEN 'CI Build & Test Auto'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%github-actions-alternative%' 
    OR bizible_form_url LIKE '%github-actions-alternative%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%github-actions-alternative%'
    OR bizible_ad_campaign_name LIKE '%octocat%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_OctoCat%')
    THEN 'OctoCat'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%' 
    OR bizible_form_url LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' and BIZIBLE_AD_CONTENT LIKE '%french%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_French%')
    THEN 'CI Use Case - FR'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%' 
    OR bizible_form_url LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
    OR (bizible_ad_campaign_name LIKE '%singleappci%' and BIZIBLE_AD_CONTENT LIKE '%paesslergerman%')))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%Singleappci_German%')
    THEN 'CI Use Case - DE'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%use-continuous-integration-to-build-and-test-faster%' 
    OR bizible_form_url LIKE '%use-continuous-integration-to-build-and-test-faster%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%use-continuous-integration-to-build-and-test-faster%'
    OR bizible_ad_campaign_name LIKE '%singleappci%'))
    OR bizible_ad_campaign_name ='20201013_ActualTechMedia_DeepMonitoringCI'
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_CI%'
        OR bizible_ad_campaign_name ILIKE '%singleappci%'))
    THEN 'CI Use Case'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%shift-your-security-scanning-left%' 
    OR bizible_form_url LIKE '%shift-your-security-scanning-left%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%shift-your-security-scanning-left%'
    OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
    OR camp.campaign_parent_id = '7014M000001dnVOQAY' -- GCP Partner campaign
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name ILIKE '%_DevSecOps%'
        OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
    THEN 'DevSecOps Use Case'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%aws-gitlab-serverless%' 
    OR bizible_landing_page LIKE '%trek10-aws-cicd%'
    OR bizible_form_url LIKE '%aws-gitlab-serverless%' 
    OR bizible_form_url LIKE '%trek10-aws-cicd%'
    OR BIZIBLE_REFERRER_PAGE LIKE '%aws-gitlab-serverless%'
    OR bizible_ad_campaign_name LIKE '%awspartner%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_AWS%')
    THEN 'AWS'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%simplify-collaboration-with-version-control%' 
    OR bizible_form_url LIKE '%simplify-collaboration-with-version-control%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%simplify-collaboration-with-version-control%'
    OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_VCC%'
        OR bizible_ad_campaign_name LIKE '%vccusecase%'))
    THEN 'VCC Use Case'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_landing_page LIKE '%gitops-infrastructure-automation%' 
    OR bizible_form_url LIKE '%gitops-infrastructure-automation%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%gitops-infrastructure-automation%'
    OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND (bizible_ad_campaign_name LIKE '%_GitOps%'
        OR bizible_ad_campaign_name LIKE '%iacgitops%'))
    THEN 'GitOps Use Case'
 When  (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_ad_campaign_name LIKE '%evergreen%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=evergreen%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=evergreen%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=evergreen%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Evergreen%')
   Then 'Evergreen'
 When (bizible_touchpoint_type = 'Web Form' 
    AND (bizible_ad_campaign_name LIKE 'brand%'
    OR bizible_ad_campaign_name LIKE 'Brand%'
    OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=brand%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=brand%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=brand%'))
    OR (camp.campaign_parent_id = '7014M000001dn8MQAQ'
    AND bizible_ad_campaign_name ILIKE '%_Brand%')
   Then 'Brand'
When (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 332
    AND (bizible_landing_page LIKE '%contact-us-ultimate%' 
    OR bizible_form_url LIKE '%contact-us-ultimate%' 
    OR BIZIBLE_REFERRER_PAGE LIKE '%contact-us-ultimate%'
    OR bizible_ad_campaign_name LIKE '%premtoultimatesp%'))
    THEN 'Premium to Ultimate'
When (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( BIZIBLE_FORM_URL_RAW LIKE '%webcast-gitops-multicloudapp%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'))
    OR (camp.campaign_parent_id LIKE '%7014M000001dpmf%')
   Then 'GitOps GTM webcast'
When (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
    AND ( BIZIBLE_FORM_URL_RAW LIKE '%devopsgtm%'
    OR BIZIBLE_LANDING_PAGE_RAW LIKE '%devopsgtm%'
    OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%devopsgtm%'))
    OR camp.campaign_parent_id LIKE '%7014M000001dpT9%'
      -- OR camp.campaign_parent_id LIKE '%7014M000001dn8M%')
    OR camp.campaign_id LIKE '%7014M000001vbtw%'
   Then 'DevOps GTM'
  Else 'None'
END                                                                                                     AS bizible_integrated_campaign_grouping,
      IFF(bizible_integrated_campaign_grouping <> 'None','Demand Gen','Other')                          AS touchpoint_segment,
      CASE
        WHEN bizible_integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat','Premium to Ultimate') 
          THEN 'CI/CD'
        WHEN bizible_integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps', 'DevOps GTM') 
          THEN 'DevOps'
        WHEN bizible_integrated_campaign_grouping IN ('GitOps Use Case','GitOps GTM webcast')  
          THEN 'GitOps'
        ELSE NULL
      END                                                                                               AS gtm_motion,
      CASE
        WHEN touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoint overrides
          THEN 'Field Event'
        WHEN bizible_marketing_channel_path = 'CPC.AdWords'
          THEN 'Google AdWords'
        WHEN bizible_marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN bizible_marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
          OR (bizible_medium = 'Field Event (old)' AND bizible_marketing_channel_path = 'Other')
          THEN 'Field Event'
        WHEN bizible_marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
          THEN 'Paid Social'
        WHEN bizible_marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
          THEN 'Social'
        WHEN bizible_marketing_channel_path IN ('Marketing Site.Web Referral','Web Referral')
          THEN 'Web Referral'
        WHEN bizible_marketing_channel_path IN ('Marketing Site.Web Direct', 'Web Direct') -- Added to Web Direct
          OR dim_campaign_id IN (
                                 '701610000008ciRAAQ', -- Trial - GitLab.com
                                 '70161000000VwZbAAK', -- Trial - Self-Managed
                                 '70161000000VwZgAAK', -- Trial - SaaS
                                 '70161000000CnSLAA0', -- 20181218_DevOpsVirtual
                                 '701610000008cDYAAY'  -- 2018_MovingToGitLab
                                )
          THEN 'Web Direct'
        WHEN bizible_marketing_channel_path LIKE 'Organic Search.%'
          OR bizible_marketing_channel_path = 'Marketing Site.Organic'
          THEN 'Organic Search'
        WHEN bizible_marketing_channel_path IN ('Sponsorship')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
      END                                                                                                 AS integrated_campaign_grouping

    FROM bizible 
    LEFT JOIN campaign
      ON bizible.campaign_id = campaign.dim_campaign_id

 )

{{ dbt_audit(
    cte_ref="touchpoints_with_campaign",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2021-03-02",
    updated_date="2021-07-20"
) }}
