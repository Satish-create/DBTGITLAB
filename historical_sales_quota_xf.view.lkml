view: historical_sales_quota_xf {
  sql_table_name: analytics.historical_sales_quota_xf ;;

  dimension: key {
    primary_key: yes
    type: string
    hidden: yes
    sql: md5(${account_owner_id} || ${quota_month_month}) ;;
  }

  dimension: account_owner {
    type: string
    sql: ${TABLE}.account_owner ;;
  }

  dimension: account_owner_id {
    type: string
    sql: ${TABLE}.account_owner_id ;;
  }

  dimension: account_owner_name {
    type: string
    sql: ${TABLE}.account_owner_name ;;
  }

  measure: quota {
    type: sum
    sql: ${TABLE}.quota ;;
  }

  dimension_group: quota_month {
    type: time
    timeframes: [
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.quota_month ;;
  }

  measure: count {
    type: count
    drill_fields: [account_owner_name]
  }
}
