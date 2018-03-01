view: dim_opportunitystage {
  sql_table_name: analytics.dim_opportunitystage ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: apiname {
    type: string
    sql: ${TABLE}.apiname ;;
  }

  dimension: createdbyid {
    type: string
    sql: ${TABLE}.createdbyid ;;
  }

  dimension_group: createddate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.createddate ;;
  }

  dimension: defaultprobability {
    type: number
    sql: ${TABLE}.defaultprobability ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: forecastcategory {
    type: string
    sql: ${TABLE}.forecastcategory ;;
  }

  dimension: forecastcategoryname {
    type: string
    sql: ${TABLE}.forecastcategoryname ;;
  }

  dimension: isactive {
    type: yesno
    sql: ${TABLE}.isactive ;;
  }

  dimension: isclosed {
    type: yesno
    sql: ${TABLE}.isclosed ;;
  }

  dimension: iswon {
    type: yesno
    sql: ${TABLE}.iswon ;;
  }

  dimension: lastmodifiedbyid {
    type: string
    sql: ${TABLE}.lastmodifiedbyid ;;
  }

  dimension_group: lastmodifieddate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastmodifieddate ;;
  }

  dimension: masterlabel {
    type: string
    sql: ${TABLE}.masterlabel ;;
  }

  dimension: sfdc_id {
    type: string
    sql: ${TABLE}.sfdc_id ;;
  }

  dimension: sortorder {
    type: number
    sql: ${TABLE}.sortorder ;;
  }

  dimension: systemmodstamp {
    type: string
    sql: ${TABLE}.systemmodstamp ;;
  }

  measure: count {
    type: count
    drill_fields: [id, forecastcategoryname, apiname]
  }
}
