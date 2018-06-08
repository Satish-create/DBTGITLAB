view: f_opportunity {
  sql_table_name: analytics.f_opportunity ;;

  dimension: account_id {
    description: "This is the foreign key to dim_account"
    hidden: yes
    type: number
    sql: ${TABLE}.account_id ;;
  }

  dimension: acv {
    hidden: yes
    type: number
    sql: ${TABLE}.acv ;;
  }

  dimension: ownerid {
    hidden: yes
    type: string
    sql:${TABLE}.ownerid ;;
  }

  dimension: days_in_stage {
    type: number
    sql: ${TABLE}.days_in_stage ;;
  }

  dimension: risk_level {
    label: "Is Risky"
    type: yesno
    sql: ${TABLE}.is_risky ;;
  }

  dimension: last_activity{
    type: date
    sql: ${TABLE}.lastactivitydate ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: competitor_unpacked {
    label: "Competitors - Unpacked"
    description: "Warning! This will cause double counting of opportunities and values because the opportunity is copied for each competitor listed!"
    type: string
    sql: unnest(string_to_array(${TABLE}.competitors__c, ';')) ;;
    # drill_fields: [detail*]
    link: {
      label: "Explore from here"
      url: "https://gitlab.looker.com/explore/sales/f_opportunity?f[f_opportunity.closedate_date]={{ _filters['f_opportunity.closedate_date'] | url_encode }}&f[dim_opportunitystage.mapped_stage]={{ _filters['dim_opportunitystage.mapped_stage'] | url_encode }}&f[dim_opportunitystage.isclosed]={{ _filters['dim_opportunitystage.isclosed'] | url_encode }}&f[f_opportunity.competitors]=%25{{ value }}%25&fields=f_opportunity.opportunity_name,f_opportunity.opportunity_type,dim_opportunitystage.mapped_stage,f_opportunity.iacv"
    }
  }

  dimension: competitors {
    label: "Competitors List"
    type: string
    sql: ${TABLE}.competitors__c ;;
  }

  dimension: iacv {
    type: number
    hidden: yes
    sql: ${TABLE}.iacv ;;
  }

  dimension: lead_source_id {
    hidden: yes
    type: number
    sql: ${TABLE}.lead_source_id ;;
  }

  dimension_group: closedate {
    label: "Opportunity Close"
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.opportunity_closedate ;;
  }

  dimension: opportunity_id {
    label: "SFDC Opportunity ID"
    description: "The 18 char SFDC Opportunity ID"
    type: string
    sql: ${TABLE}.opportunity_id ;;
  }

  dimension: opportunity_name {
    label: "SFDC Opportunity Name"
    description: "The name of the opportunity record from Salesforce."
    type: string
    sql: ${TABLE}.opportunity_name ;;

    link: {
      label: "Salesforce Opportunity"
      url: "https://na34.salesforce.com/{{ f_opportunity.opportunity_id._value }}"
      icon_url: "https://c1.sfdcstatic.com/etc/designs/sfdc-www/en_us/favicon.ico"
    }

  }

  dimension: opportunity_product {
    label: "Product Name"
    type: string
    sql: ${TABLE}.opportunity_product ;;
  }

  dimension: opportunity_sales_segmentation {
    label: "Opportunity Sales Segmentation"
    type: string
    sql: ${TABLE}.opportunity_sales_segmentation ;;
  }

  dimension: opportunity_stage_id {
    description: "The foreign key to dim_opportunitystage"
    hidden: yes
    type: number
    sql: ${TABLE}.opportunity_stage_id ;;
  }

  dimension: opportunity_type {
    label: "Opportunity Type"
    description: "The SFDC opportunity type (New, Renewal,Add-On Business)"
    type: string
    sql: ${TABLE}.opportunity_type ;;
  }

  dimension: quantity {
    label: "Product Quantity"
    type: number
    sql: ${TABLE}.quantity ;;
  }

  dimension: renewal_acv {
    hidden: yes
    type: number
    sql: ${TABLE}.renewal_acv ;;
  }

  dimension_group: sales_accepted {
    label: "Sales Accepted"
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.sales_accepted_date ;;
  }

  dimension_group: sales_qualified {
    label: "Sales Qualified"
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.sales_qualified_date ;;
  }

  dimension: sales_qualified_source {
    description: "Sales Qualified Source"
    type: string
    sql: ${TABLE}.sales_qualified_source ;;
  }

  dimension: tcv {
    label: "Total Contract Value"
    hidden: yes
    type: number
    sql: ${TABLE}.tcv ;;
  }

  measure: number_of_opportunities {
    label: "Count of Opportunities"
    type: count_distinct
    sql: ${opportunity_id} ;;
    drill_fields: [detail*]
  }

  measure: total_tcv {
    label: "Total Contract Value (TCV)"
    type: sum
    sql: ${tcv} ;;
    drill_fields: [detail*]
    value_format_name: usd
  }

  measure: total_acv {
    label: "Total Annual Contract Value (ACV)"
    type: sum
    sql: ${acv} ;;
    drill_fields: [detail*]
    value_format_name: usd
    }

  measure: total_iacv {
    label: "Total Incremental Annual Contract Value (IACV)"
    type: sum
    sql: ${iacv} ;;
    drill_fields: [detail*]
    value_format_name: usd
    }

  measure: weighted_iacv {
    label: "Total Weighted IACV"
    type: sum
    sql:${iacv}* (${dim_opportunitystage.defaultprobability}/100) ;;
  }

  measure: total_sqos {
    label: "Total Sales Qualified Opportunities (SQOs)"
    type: count_distinct
    sql:  ${opportunity_id} ;;
    filters: {
      field: dim_leadsource.initial_source
      value: "-Web Direct"
    }
    filters: {
      field: iacv
      value: ">=0"
    }
    filters: {
      field: sales_qualified_date
      value: "-NULL"
    }
    drill_fields: [detail*]
  }

  measure: total_saos {
    label: "Total Sales Accepted Opportunities (SAOs)"
    type: count_distinct
    sql:  ${opportunity_id} ;;
    filters: {
      field: dim_leadsource.initial_source
      value: "-Web Direct"
    }
    filters: {
      field: iacv
      value: ">=0"
    }
    filters: {
      field: sales_accepted_date
      value: "-NULL"
    }
    drill_fields: [detail*]
  }

  measure: total_sao_iacv {
    label: "Total Sales Accepted Opportunity (SAO) IACV"
    type: sum
    sql:  ${iacv} ;;
    filters: {
      field: dim_leadsource.initial_source
      value: "-Web Direct"
    }
    filters: {
      field: iacv
      value: ">=0"
    }
    filters: {
      field: sales_accepted_date
      value: "-NULL"
    }
    drill_fields: [detail*]
  }

  measure: total_sqo_iacv {
    label: "Total Sales Qualified Opportunity (SQO) IACV "
    type: sum
    sql:   ${iacv} ;;
    filters: {
      field: dim_leadsource.initial_source
      value: "-Web Direct"
    }
    filters: {
      field: iacv
      value: ">=0"
    }
    filters: {
      field: sales_qualified_date
      value: "-NULL"
    }
    drill_fields: [detail*]
  }

  measure: opp_renewal_acv {
    label: "Renewal ACV"
    type: sum
    sql: ${TABLE}.renewal_acv ;;
    drill_fields: [detail*]
    value_format_name: usd
  }

  measure: total_quantity {
    label: "Total Quantity"
    type: sum
    sql: ${quantity} ;;
    drill_fields: [detail*]
  }

  set: detail {
    fields: [
      dim_account.name, opportunity_name, opportunity_sales_segmentation, opportunity_type, closedate_date, total_iacv, total_acv
    ]
  }
}
