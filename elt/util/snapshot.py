#!/usr/bin/python3
import psycopg2
import os

try:
    connect_str = "dbname=" + os.environ['PG_DATABASE'] + " user=" + \
                  os.environ['PG_USERNAME'] + \
                  " host=" + os.environ['PG_ADDRESS'] + \
                  " password=" + os.environ['PG_PASSWORD']

    # create connection
    conn = psycopg2.connect(connect_str)

    # create cursor
    cursor = conn.cursor()
    check_sql = "SELECT count(*) as cnt FROM sfdc_derived.ss_opportunity where " +\
                "snapshot_date::date = (current_date at time zone 'US/Pacific' - interval '1 day')::date"
    cursor.execute(check_sql)
    rs = cursor.fetchone()
    if rs[0] > 1:
        print("Snapshot exists. Passing.")
        cursor.close()
        pass
    else:
        print("No snapshot found. Creating one.")
        cursor = conn.cursor()
        sql = "INSERT INTO sfdc_derived.ss_opportunity " + \
              "SELECT (current_date at time zone 'US/Pacific' - interval '1 day')::date as snapshot_date, " + \
              "o.id," + \
              "o.of_users_on_ce__c," + \
              "o.x0_pending_acceptance_date__c," + \
              "o.x1_discovery_date__c," + \
              "o.x2_scoping_date__c," + \
              "o.x3_technical_evaluation_date__c," + \
              "o.x4_proposal_date__c," + \
              "o.x5_negotiating_date__c," + \
              "o.x6_closed_won_date__c," + \
              "o.x7_closed_lost_date__c," + \
              "o.x8_unqualified_date__c," + \
              "o.x9_duplicate_date__c," + \
              "o.accountid," + \
              "o.activity_source__c," + \
              "o.acv__c," + \
              "o.acv_2__c," + \
              "o.acv_override__c," + \
              "o.ae_qualification_status__c," + \
              "o.amount," + \
              "o.approval_status__c," + \
              "o.arr__c," + \
              "o.arr_override__c," + \
              "o.authorized_reseller_opportunity__c," + \
              "o.auto_renew__c," + \
              "o.auto_renewal__c," + \
              "o.awaiting_approval_stage__c," + \
              "o.bdr_qualified_stage__c," + \
              "o.booking_category__c," + \
              "o.business_development_rep__c," + \
              "o.bdr_lu__c," + \
              "o.business_problems_to_solve__c," + \
              "o.business_value_assessment__c," + \
              "o.buying_process_for_procuring_gitlab__c," + \
              "o.campaignid," + \
              "o.champion_selling_on_your_behalf__c," + \
              "o.churn_acv__c," + \
              "o.churn_month__c," + \
              "o.churn_notes__c," + \
              "o.churn_type__c," + \
              "o.closedate," + \
              "o.isclosed," + \
              "o.competitive_awareness__c," + \
              "o.competitors__c," + \
              "o.converted_from_ce__c," + \
              "o.count_of_active_ee_subscriptions__c," + \
              "o.count_of_primary_contacts__c," + \
              "o.count_of_products__c," + \
              "o.count_of_quotes__c," + \
              "o.count_of_quotes_to_z_billing__c," + \
              "o.count_of_quotes_with_invoice_owner__c," + \
              "o.count_of_signed_agreements__c," + \
              "o.create_renewal_opportunity__c," + \
              "o.createdbyid," + \
              "o.createddate," + \
              "o.created_month__c," + \
              "o.zqu__currentgenerators__c," + \
              "o.currently_using_ce__c," + \
              "o.data_quality_description__c," + \
              "o.data_quality_score__c," + \
              "o.days_in_0_pending_acceptance__c," + \
              "o.days_in_1_discovery__c," + \
              "o.days_in_2_scoping__c," + \
              "o.days_in_3_technical_evaluation__c," + \
              "o.days_in_4_proposal__c," + \
              "o.days_in_5_negotiating__c," + \
              "o.leandata__days_in_stage__c," + \
              "o.days_since_customer__c," + \
              "o.leandata__days_to_close__c," + \
              "o.days_until_close__c," + \
              "o.dec_criteria_tech_vendor_financial__c," + \
              "o.dec_process_define_validation_approval__c," + \
              "o.decision_criteria__c," + \
              "o.decision_making_process__c," + \
              "o.isdeleted," + \
              "o.zqu__deliveryinstallationstatus__c," + \
              "o.delta_amount__c," + \
              "o.description," + \
              "o.developing_stage__c," + \
              "o.discovery_stage__c," + \
              "o.don_t_send_slack_notification__c," + \
              "o.economic_buyer_who_has_profit_loss__c," + \
              "o.ee_trial_start_date__c," + \
              "o.sertifi2_0__email__c," + \
              "o.end_date__c," + \
              "o.engagement_type__c," + \
              "o.exclude_number_of_opps_is__c," + \
              "o.exclude_sales_cycle_analysis__c," + \
              "o.exclude_sales_cycle_analysis_is__c," + \
              "o.exclude_won_loss_analysis_is__c," + \
              "o.expected_amount__c," + \
              "o.expected_close_date__c," + \
              "o.first_12_months__c," + \
              "o.forecastcategoryname," + \
              "o.forecastcategory," + \
              "o.full_circle_count__c," + \
              "o.full_circle_velocity__c," + \
              "o.funding__c," + \
              "o.hasopportunitylineitem," + \
              "o.hasopenactivity," + \
              "o.hasoverduetask," + \
              "o.how_are_you_solving_these_problems_today__c," + \
              "o.how_large_is_your_group__c," + \
              "o.how_many_seats_are_they_interested_in__c," + \
              "o.identify_pain__c," + \
              "o.if_svn_why_looking_to_move_to_git__c," + \
              "o.impact_due_to_business_problems__c," + \
              "o.incremental_acv__c," + \
              "o.incremental_acv_licenses__c," + \
              "null," + \
              "null," + \
              "o.incremental_acv_2__c," + \
              "o.incremental_acv_override__c," + \
              "o.incremental_amount__c," + \
              "o.incremental_amount_2__c," + \
              "o.incremental_arr__c," + \
              "o.infer__infer_band__c," + \
              "o.infer__infer_hash__c," + \
              "o.infer__infer_last_modified__c," + \
              "o.infer__infer_rating__c," + \
              "o.infer__infer_score__c," + \
              "o.infer__infer_won_amount__c," + \
              "o.initial_term__c," + \
              "o.interested_in_gitlab_ee__c," + \
              "o.interested_in_hosted_solution__c," + \
              "o.internal_champion__c," + \
              "o.invoice_amount__c," + \
              "o.invoice_number__c," + \
              "o.invoice_paid_date__c," + \
              "o.invoice_paid__c," + \
              "o.is_0_pending_acceptance__c," + \
              "o.is_1_discovery__c," + \
              "o.is_2_scoping__c," + \
              "o.is_3_technical_evaluation__c," + \
              "o.is_4_proposal__c," + \
              "o.is_5_negotiating__c," + \
              "o.is_6_closed_won__c," + \
              "o.is_7_closed_lost__c," + \
              "o.is_8_unqualified__c," + \
              "o.is_9_duplicate__c," + \
              "o.where_is_the_budget_for_this_coming_from__c," + \
              "o.lastactivitydate," + \
              "o.lastmodifiedbyid," + \
              "o.lastmodifieddate," + \
              "o.lastreferenceddate," + \
              "o.lastvieweddate," + \
              "o.leadsource," + \
              "o.lead_activity_type__c," + \
              "o.legacy_sql_new_biz__c," + \
              "o.islost__c," + \
              "o.lost_amount__c," + \
              "o.zqu__maincompetitors__c," + \
              "o.mkto_si__marketoanalyzer__c," + \
              "o.max_ticket_group_from_products_numeric__c," + \
              "o.is_there_a_meeting_set_with_an_ae_to_dis__c," + \
              "o.metrics__c," + \
              "o.missing_features__c," + \
              "o.mrr__c," + \
              "o.mrr_override__c," + \
              "o.mrr1__c," + \
              "o.multiyear_renewal__c," + \
              "o.name," + \
              "o.need_to_buy__c," + \
              "o.negotiating_stage__c," + \
              "null," + \
              "o.nextstep," + \
              "o.nrv__c," + \
              "o.null_date__c," + \
              "o.number_of_pushes__c," + \
              "o.online_purchase__c," + \
              "o.isopen__c," + \
              "o.open_amount__c," + \
              "o.opportunity__c," + \
              "o.opportunity_age__c," + \
              "o.opportunity_closed__c," + \
              "o.opportunity_term_new__c," + \
              "o.opportunity_term__c," + \
              "o.opportunity_term_override__c," + \
              "o.type," + \
              "o.zqu__ordernumber__c," + \
              "o.leandata__ordernumber__c," + \
              "o.owner_account_type__c," + \
              "o.ownerid," + \
              "o.partners__c," + \
              "o.paymentupdateddate__c," + \
              "o.poc_end_date__c," + \
              "o.poc_notes__c," + \
              "o.poc_start_date__c," + \
              "o.poc_success_criteria__c," + \
              "o.present_solution_stage__c," + \
              "o.pricebook2id," + \
              "null," + \
              "null," + \
              "o.probability," + \
              "o.product_details__c," + \
              "o.projections_growth__c," + \
              "o.projections_new__c," + \
              "o.projections_total__c," + \
              "o.proof_of_concept_poc_status__c," + \
              "o.what_problems_are_you_looking_to_solve__c," + \
              "o.purchasing_procurement_process__c," + \
              "o.push_counter__c," + \
              "o.qualification_notes__c," + \
              "o.quote_amount__c," + \
              "o.syncedquoteid," + \
              "o.reason_for_lost__c," + \
              "o.reason_for_lost_details__c," + \
              "o.reason_for_refund_credit__c," + \
              "o.reason_we_won__c," + \
              "o.recordtypeid," + \
              "o.refund_opp_source__c," + \
              "o.region_o__c," + \
              "o.related_quote__c," + \
              "o.related_subscription__c," + \
              "o.renewal_acv__c," + \
              "o.renewal_amount__c," + \
              "o.renewal_arr__c," + \
              "o.renewal_mrr__c," + \
              "o.renewal_notification__c," + \
              "o.renewal_term__c," + \
              "o.leandata__reporting_last_run_date__c," + \
              "o.leandata__reporting_opportunity_source__c," + \
              "o.leandata__reporting_total_marketing_touches__c," + \
              "o.leandata__reporting_won_number__c," + \
              "o.role_prospect_plays_in_evaluation__c," + \
              "o.sales_accepted_date__c," + \
              "o.sales_cycle_key__c," + \
              "o.sdr_lu__c," + \
              "o.sales_qualified__c," + \
              "o.sql_amount__c," + \
              "o.sales_qualified_date__c," + \
              "o.sales_qualified_fiscal_period__c," + \
              "o.sql_source__c," + \
              "o.sales_segmentation_o__c," + \
              "o.sdr__c," + \
              "o.second_owner_insight_squared__c," + \
              "o.software_development_methodology_state__c," + \
              "o.solution_fit__c," + \
              "o.solutions_to_be_replaced__c," + \
              "o.solutions_to_be_replaced_notes__c," + \
              "null," + \
              "o.stagename," + \
              "o.stage_report__c," + \
              "o.start_date__c," + \
              "o.sub_region_o__c," + \
              "o.subscription_start_date__c," + \
              "o.systemmodstamp," + \
              "o.term_end_date__c," + \
              "o.term_start_date__c," + \
              "o.zqu__trackingnumber__c," + \
              "o.what_vcs_are_you_using_now__c," + \
              "o.what_performance_issues_do_you_have__c," + \
              "o.how_are_you_using_gitlab__c," + \
              "o.what_other_groups_are_using_git__c," + \
              "o.what_is_the_timing_to_purchase__c," + \
              "o.what_would_constitute_a_successful_trial__c," + \
              "o.won_amount__c," + \
              "o.unqualified_amount__c," + \
              "o.account_owner__c," + \
              "o.isunqualified__c," + \
              "o.what_is_prospect_doing_to_address_need__c," + \
              "o.who_is_the_decision_maker_for_gitlab__c," + \
              "o.zuora_mrr__c," + \
              "o.zuora_initial_term__c," + \
              "o.web_portal_purchase__c," + \
              "o.trigger_workflow__c," + \
              "o.type_amount_close_date__c," + \
              "o.why_did_you_choose_gitlab__c," + \
              "o.verbal_commitment_stage__c," + \
              "o.products_purchased__c," + \
              "o.weighted_iacv__c," + \
              "o.vertical_market_qualification__c," + \
              "o.win_probability_score__c," + \
              "o.xactly_renewal_amount__c," + \
              "o.xactly_incremental_amount__c," + \
              "o.xactly_invoice_paid_date__c," + \
              "o.initial_iacv__c," + \
              "o.ultimate_parent_sales_segment_o__c," + \
              "o.opportunity_owner__c," + \
              "null," + \
              "o.__row_id," + \
              "o.channel_partner_stage_override__c," + \
              "o.acv_test__c," + \
              "o.does_not_require_approval__c," + \
              "o.channel_partner_close_date_override__c," + \
              "o.fiscal," + \
              "o.mrr2__c," + \
              "o.account_id__c," + \
              "o.zqu__zuoraconfig__c," + \
              "o.fiscalyear," + \
              "o.account_id_18__c," + \
              "o.fiscalquarter," + \
              "o.iswon," + \
              "o.channel_manager__c," + \
              "o.channel_partner_iacv_override__c " + \
              "o.upside_iacv__c " + \
              "o.upside_swing_deal_iacv__c " + \
              "o.merged_opportunity__c " + \
              "FROM sfdc.opportunity o WHERE isdeleted=FALSE"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    conn.close()

except Exception as e:
    print('There was an error snapshotting data', e)
    raise
