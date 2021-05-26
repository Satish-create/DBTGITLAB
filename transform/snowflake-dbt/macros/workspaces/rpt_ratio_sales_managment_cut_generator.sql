{% macro rpt_ratio(select_columns, is_new_logo_calc, extra_where_clause='TRUE') %}

{%- set metrics = 
    ["MQLs / A",
    "MQLs / TQ",
    "MQLs / QTD",
    "Trials / A",
    "Trials / TQ",
    "Trials / QTD",
    "First Order ARR / A",
    "First Order ARR / TQ",
    "First Order ARR / QTD",
    "New Logos / A",
    "New Logos / TQ",
    "New Logos / QTD",
    "SAOs / A",
    "SAOs / TQ",
    "SAOs / QTD",
    "Won Opps / A",
    "Won Opps / TQ",
    "Won Opps / QTD",
    "Total Opps / A",
    "Total Opps / TQ",
    "Total Opps / QTD"]
  -%}

{% set large_segment_region_grouped = ['APAC', 'EMEA', 'Large Other', 'PubSec', 'US East', 'US West', 'Pubsec', 'Global', 'Large MQLs & Trials'] %}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('rpt_crm_opportunity_closed_period', 'rpt_crm_opportunity_closed_period'),
    ('rpt_crm_opportunity_accepted_period', 'rpt_crm_opportunity_accepted_period'),
    ('rpt_crm_person_mql', 'rpt_crm_person_mql'),
    ('rpt_sales_funnel_target', 'rpt_sales_funnel_target'),
    ('rpt_sales_funnel_target_daily', 'rpt_sales_funnel_target_daily')
]) }}


, crm_person AS (

    SELECT
      {{ dbt_utils.star(from=ref('rpt_crm_person_mql'), except=['SALES_SEGMENT_GROUPED']) }},
      {{ null_or_missing('sales_segment_name', 'sales_segment') }},
      {{ null_or_missing('sales_segment_region_mapped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_person_mql
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{ extra_where_clause }}
  
), crm_opportunity_closed_period AS (

    SELECT
      rpt_crm_opportunity_closed_period.*,
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped', 'sales_segment') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_region_stamped_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_opportunity_closed_period
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), crm_opportunity_accepted_period AS (

    SELECT 
      rpt_crm_opportunity_accepted_period.*,
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_opportunity_accepted_period
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), sales_funnel_target AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_sales_funnel_target
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), sales_funnel_target_daily AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target_daily'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_sales_funnel_target_daily
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
      AND {{extra_where_clause}}

), current_fiscal_quarter AS (
  
    SELECT DISTINCT fiscal_quarter_name_fy AS current_fiscal_quarter
    FROM dim_date
    WHERE date_actual = CURRENT_DATE

), factor_to_date AS (
  
    SELECT DISTINCT
      'date_range_quarter' AS _type,
      fiscal_quarter_name_fy::VARCHAR AS _date,
      IFF(fiscal_quarter_name_fy < current_fiscal_quarter.current_fiscal_quarter, TRUE, FALSE) AS is_selected_quarter_lower_than_current_quarter,
      last_day_of_fiscal_quarter
    FROM dim_date
    LEFT JOIN current_fiscal_quarter
    -- WHERE [fiscal_quarter_name_fy=bc_fiscal_quarter]
    WHERE fiscal_quarter_name_fy='FY22-Q1'

), prep_base_list AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM crm_opportunity_closed_period
    
    UNION
    
    --SAOs
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM crm_opportunity_accepted_period
    
    UNION
    
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM crm_person
    
    UNION
    
    -- Targets
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM sales_funnel_target
    
    UNION
    
    -- Targets MQL
    SELECT 
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM sales_funnel_target
    WHERE kpi_name IN ('MQL', 'Trials')
  
), base_list AS (

    SELECT *
    FROM prep_base_list

    {% if select_columns|count > 1 %}

      {% for __ in select_columns %}

      UNION

      SELECT
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
          {% if loop.first %}
            {{select_column}}
          {% else %}
            'Total'
          {% endif %}
          {% if not loop.last %},{% endif %}

        {% endfor %}
      FROM prep_base_list

      {% endfor %}

    {% endif %}

    UNION

    SELECT
      fiscal_quarter_name_fy,
      {% for __ in select_columns %}
        'Total' {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM prep_base_list

), new_logos_actual AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT dim_crm_opportunity_id)               AS actual_new_logos
    FROM crm_opportunity_closed_period
    WHERE is_won = 'TRUE'
      AND is_closed = 'TRUE'
      AND is_edu_oss = 0
      -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
      AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
    GROUP BY ROLLUP( 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )

), sao_count AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(*)                AS saos
    FROM crm_opportunity_accepted_period
    WHERE is_sao = TRUE
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), first_oder_arr_closed_won AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(net_arr)            AS first_oder_arr_closed_won
    FROM crm_opportunity_closed_period
    WHERE is_won = 'TRUE'
      AND is_closed = 'TRUE'
      AND is_edu_oss = 0
      --AND order_type = '1. New - First Order'
      AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
    GROUP BY ROLLUP(1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), win_rate AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(CASE WHEN is_won THEN 1 END)                            AS won_opps,
      COUNT(is_won)                                                 AS total_opps
    FROM crm_opportunity_closed_period
    WHERE is_win_rate_calc = TRUE
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
   
), mql_trial_count AS (  
  
    SELECT 
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT email_hash)             AS mqls,
      COUNT(DISTINCT IFF(is_lead_source_trial, email_hash || lead_source, NULL)) AS actual_trials
    FROM crm_person
    GROUP BY ROLLUP( 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), targets AS (
 
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(IFF(kpi_name = 'Trials', qtd_allocated_target, 0)) AS  target_trials,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', qtd_allocated_target, 0)) AS  target_new_logos,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', qtd_allocated_target, 0)) AS  target_net_arr_closed,
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', qtd_allocated_target, 0)) AS  target_new_logos,
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', qtd_allocated_target, 0)) AS  target_net_arr_closed,
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', qtd_allocated_target, 0)) AS  target_sao,
      SUM(IFF(kpi_name = 'MQL', qtd_allocated_target, 0)) AS  target_mql,
      SUM(IFF(kpi_name = 'Deals', qtd_allocated_target, 0)) AS  target_won_opps,
      SUM(IFF(kpi_name = 'Total Closed', qtd_allocated_target, 0)) AS  target_total_opps
    
    FROM sales_funnel_target_daily
    LEFT JOIN factor_to_date
    WHERE IFF(factor_to_date.is_selected_quarter_lower_than_current_quarter, target_date = factor_to_date.last_day_of_fiscal_quarter, report_target_date = CURRENT_DATE)
      --AND [fiscal_quarter_name_fy=bc_fiscal_quarter]
      AND fiscal_quarter_name_fy='FY22-Q1'
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  
), targets_full AS (
 
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        IFNULL({{ select_column }}, 'Total') AS {{ select_column }},
      {% endfor %}
      SUM(IFF(kpi_name = 'Trials', allocated_target, 0)) AS  target_trials_full,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', allocated_target, 0)) AS  target_new_logos_full,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', allocated_target, 0)) AS  target_net_arr_closed_full,
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', allocated_target, 0)) AS  target_new_logos_full,
      SUM(IFF(IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', allocated_target, 0)) AS  target_net_arr_closed_full,
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', allocated_target, 0)) AS  target_sao_full,
      SUM(IFF(kpi_name = 'MQL', allocated_target, 0)) AS  target_mql_full,
      SUM(IFF(kpi_name = 'Deals', allocated_target, 0)) AS  target_won_opps_full,
      SUM(IFF(kpi_name = 'Total Closed', allocated_target, 0)) AS  target_total_opps_full
    
    FROM sales_funnel_target
    GROUP BY ROLLUP (1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )

), agg AS (

  SELECT
    base_list.fiscal_quarter_name_fy,
    {% for select_column in select_columns %}
    base_list.{{select_column}},
    {% endfor %}
    
    IFNULL(mqls, 0)                            AS "MQLs / A",
    NULLIF(target_mql_full, 0)                 AS "MQLs / TQ",
    NULLIF(target_mql, 0)                      AS "MQLs / QTD",
    IFNULL(actual_trials, 0)                   AS "Trials / A",
    NULLIF(target_trials_full, 0)              AS "Trials / TQ",
    NULLIF(target_trials, 0)                   AS "Trials / QTD",
    IFNULL(first_oder_arr_closed_won, 0)       AS "First Order ARR / A",
    NULLIF(target_net_arr_closed_full, 0)      AS "First Order ARR / TQ",
    NULLIF(target_net_arr_closed, 0)           AS "First Order ARR / QTD",
    IFNULL(actual_new_logos, 0)                AS "New Logos / A",
    NULLIF(target_new_logos_full, 0)           AS "New Logos / TQ",
    NULLIF(target_new_logos, 0)                AS "New Logos / QTD",
    IFNULL(saos, 0)                            AS "SAOs / A",
    NULLIF(target_sao_full, 0)                 AS "SAOs / TQ",
    NULLIF(target_sao, 0)                      AS "SAOs / QTD",
    IFNULL(won_opps, 0)                        AS "Won Opps / A",
    NULLIF(target_won_opps_full, 0)            AS "Won Opps / TQ",
    NULLIF(target_won_opps, 0)                 AS "Won Opps / QTD",
    IFNULL(total_opps, 0)                      AS "Total Opps / A",
    NULLIF(target_total_opps_full, 0)          AS "Total Opps / TQ",
    NULLIF(target_total_opps, 0)               AS "Total Opps / QTD"

  FROM base_list

  INNER JOIN factor_to_date
    ON base_list.fiscal_quarter_name_fy::VARCHAR = factor_to_date._date::VARCHAR

  LEFT JOIN new_logos_actual
    ON base_list.fiscal_quarter_name_fy = new_logos_actual.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = new_logos_actual.{{select_column}}
    {% endfor %}

  LEFT JOIN first_oder_arr_closed_won
    ON base_list.fiscal_quarter_name_fy = first_oder_arr_closed_won.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = first_oder_arr_closed_won.{{select_column}}
    {% endfor %}

  LEFT JOIN mql_trial_count
    ON base_list.fiscal_quarter_name_fy = mql_trial_count.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = mql_trial_count.{{select_column}}
    {% endfor %}

  LEFT JOIN sao_count
    ON base_list.fiscal_quarter_name_fy = sao_count.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = sao_count.{{select_column}}
    {% endfor %}

  LEFT JOIN win_rate
    ON base_list.fiscal_quarter_name_fy = win_rate.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = win_rate.{{select_column}}
    {% endfor %}
  
  LEFT JOIN targets
    ON base_list.fiscal_quarter_name_fy = targets.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = targets.{{select_column}}
    {% endfor %}
  LEFT JOIN targets_full
    ON base_list.fiscal_quarter_name_fy = targets_full.fiscal_quarter_name_fy
    {% for select_column in select_columns %}
    AND base_list.{{select_column}} = targets_full.{{select_column}}
    {% endfor %}
  
  WHERE NOT (
    "First Order ARR / A" = 0 AND "First Order ARR / TQ" IS NULL
    AND "New Logos / A" = 0   AND "New Logos / TQ"       IS NULL
    AND "MQLs / A" = 0        AND "MQLs / TQ"            IS NULL
    AND "SAOs / A" = 0        AND "SAOs / TQ"            IS NULL
    AND "Won Opps / A" = 0    AND "Won Opps / TQ"        IS NULL
    AND "Total Opps / A" = 0  AND "Total Opps / TQ"      IS NULL
    AND "Trials / A" = 0      AND "Trials / TQ"          IS NULL
  )

)

{% for select_column in select_columns %}
  {% if select_column == 'segment_region_grouped' %}
  
    , large_subtotal_no_mql_trial_new_logo AS (

        SELECT
          fiscal_quarter_name_fy,
          {% for column in select_columns %}
            {% if column == 'segment_region_grouped' %}
              'Large-Total' AS {{column}},
            {% else %}
              {{column}},
            {% endif %}
          {% endfor %}
          {% for metric in metrics %}
            SUM("{{metric}}") AS "{{metric}}"
            {% if not loop.last %},{% endif %}
          {% endfor %}
        FROM agg
        WHERE segment_region_grouped IN (
          {% for large_region in large_segment_region_grouped %} '{{large_region}}' {% if not loop.last %}, {% endif %} {% endfor %}
          )
          {% for select_column in select_columns %}
            AND agg.{{select_column}} != 'Total'
          {% endfor %}
        GROUP BY 1,
          {% for column in select_columns %}
            {% if column == 'segment_region_grouped' %}
              'Large-Total'
            {% else %}
              {{column}}
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}

    ), large_subtotal_new_logo AS (

      SELECT
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
          {% if column == 'segment_region_grouped' %}
            'Large-Total' AS {{column}},
          {% else %}
            {{column}},
          {% endif %}
        {% endfor %}
        COUNT(DISTINCT dim_crm_opportunity_id)               AS "New Logos / A"
      FROM crm_opportunity_closed_period
      WHERE is_won = 'TRUE'
        AND is_closed = 'TRUE'
        AND is_edu_oss = 0
        -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
        AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
        AND segment_region_grouped IN (
          {% for large_region in large_segment_region_grouped %} '{{large_region}}' {% if not loop.last %}, {% endif %} {% endfor %}
        )
      GROUP BY 1,
        {% for column in select_columns %}
          {% if column == 'segment_region_grouped' %}
            'Large-Total'
          {% else %}
            {{column}}
          {% endif %}
          {% if not loop.last %},{% endif %}
        {% endfor %}

    ), large_subtotal AS (

        SELECT
          large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy,
          {% for column in select_columns %}
            large_subtotal_no_mql_trial_new_logo.{{column}},
          {% endfor %}
          {% for metric in metrics %}
            {% if metric == 'MQLs / A' %}
              mql_trial_count.mqls AS "{{metric}}"
            {% elif metric == 'Trials / A' %}
              mql_trial_count.actual_trials AS "{{metric}}"
            {% elif metric == 'New Logos / A' %}
              large_subtotal_new_logo."New Logos / A" AS "{{metric}}"
            {% else %}
              large_subtotal_no_mql_trial_new_logo."{{metric}}"
            {% endif %}
            {% if not loop.last %},{% endif %}
          {% endfor %}
        FROM large_subtotal_no_mql_trial_new_logo
        LEFT JOIN mql_trial_count
          ON mql_trial_count.fiscal_quarter_name_fy = large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy
          {% for select_column in select_columns %}
            {% if select_column == 'segment_region_grouped' %}
              AND IFF(mql_trial_count.segment_region_grouped LIKE 'Large%', 'Large-Total', NULL) = large_subtotal_no_mql_trial_new_logo.segment_region_grouped
            {% else %}
              AND mql_trial_count.{{select_column}} = large_subtotal_no_mql_trial_new_logo.{{select_column}}
            {% endif %}
          {% endfor %}
          AND mql_trial_count.segment_region_grouped LIKE 'Large%'
        LEFT JOIN large_subtotal_new_logo
          ON large_subtotal_new_logo.fiscal_quarter_name_fy = large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy
          {% for select_column in select_columns %}
            AND large_subtotal_new_logo.{{select_column}} = large_subtotal_no_mql_trial_new_logo.{{select_column}}
          {% endfor %}

    )
    {% endif %}
{% endfor %}

  {% if select_columns[0] == 'segment_region_grouped' and select_columns|count > 1 %}

, large_subtotal_extra_mql_trial AS (

    SELECT 
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
        {% if select_column == 'segment_region_grouped' %}
          'Large-Total' AS {{select_column}},
        {% else %}
          'Total' AS {{select_column}},
        {% endif %}
      {% endfor %}
      COUNT(DISTINCT email_hash)                                                 AS "MQLs / A",
      COUNT(DISTINCT IFF(is_lead_source_trial, email_hash || lead_source, NULL)) AS "Trials / A"
    FROM crm_person
    WHERE crm_person.segment_region_grouped LIKE 'Large%'
    GROUP BY 1,
      {% for select_column in select_columns %}
        {% if select_column == 'segment_region_grouped' %}
          'Large-Total'
        {% else %}
          'Total'
        {% endif %}
        {% if not loop.last %},{% endif %}
      {% endfor %}

    ), large_subtotal_extra_new_logo AS (

      SELECT
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
          {% if column == 'segment_region_grouped' %}
            'Large-Total' AS {{column}},
          {% else %}
            'Total' AS {{column}},
          {% endif %}
        {% endfor %}
        COUNT(DISTINCT dim_crm_opportunity_id)               AS "New Logos / A"
      FROM crm_opportunity_closed_period
      WHERE is_won = 'TRUE'
        AND is_closed = 'TRUE'
        AND is_edu_oss = 0
        -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
        AND IFF({{is_new_logo_calc}} = FALSE, TRUE, order_type = '1. New - First Order')
        AND segment_region_grouped IN (
          {% for large_region in large_segment_region_grouped %} '{{large_region}}' {% if not loop.last %}, {% endif %} {% endfor %}
        )
      GROUP BY 1,
        {% for column in select_columns %}
          {% if column == 'segment_region_grouped' %}
            'Large-Total'
          {% else %}
            'Total'
          {% endif %}
          {% if not loop.last %},{% endif %}
        {% endfor %}

    ), large_subtotal_extra_no_mql_trial_new_logo AS (

      SELECT
        fiscal_quarter_name_fy,

        {% for select_column in select_columns %}
          {% if select_column == 'segment_region_grouped' %}
            large_subtotal.{{select_column}},
          {% else %}
            'Total' AS {{select_column}},
          {% endif %}
        {% endfor %}

        {% for metric in metrics %}
          SUM(large_subtotal."{{metric}}") AS "{{metric}}"
          {% if not loop.last %},{% endif %}
        {% endfor %}
      FROM large_subtotal
      WHERE large_subtotal.segment_region_grouped = 'Large-Total'
      GROUP BY 1,
        {% for select_column in select_columns %}
          {% if select_column == 'segment_region_grouped' %}
            large_subtotal.{{select_column}},
          {% else %}
            'Total' {% if not loop.last %},{% endif %}
          {% endif %}
        {% endfor %}

), large_subtotal_extra AS (

    SELECT
      large_subtotal_extra_no_mql_trial_new_logo.fiscal_quarter_name_fy,

      {% for select_column in select_columns %}
        large_subtotal_extra_no_mql_trial_new_logo.{{select_column}},
      {% endfor %}

      {% for metric in metrics %}
        {% if metric == 'MQLs / A' or metric == 'Trials / A' %}
          large_subtotal_extra_mql_trial."{{metric}}"
        {% elif metric =='New Logos / A' %}
          large_subtotal_extra_new_logo."{{metric}}"
        {% else %}
          large_subtotal_extra_no_mql_trial_new_logo."{{metric}}"
        {% endif %}
        {% if not loop.last %},{% endif %}
      {% endfor %}
    FROM large_subtotal_extra_no_mql_trial_new_logo
    LEFT JOIN large_subtotal_extra_mql_trial
      ON large_subtotal_extra_no_mql_trial_new_logo.fiscal_quarter_name_fy = large_subtotal_extra_mql_trial.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
        AND large_subtotal_extra_no_mql_trial_new_logo.{{select_column}} = large_subtotal_extra_mql_trial.{{select_column}}
      {% endfor %}
    LEFT JOIN large_subtotal_extra_new_logo
      ON large_subtotal_extra_new_logo.fiscal_quarter_name_fy = large_subtotal_extra_mql_trial.fiscal_quarter_name_fy
      {% for select_column in select_columns %}
        AND large_subtotal_extra_new_logo.{{select_column}} = large_subtotal_extra_mql_trial.{{select_column}}
      {% endfor %}

)

  {% endif %}

, final AS (

SELECT *
FROM agg

{% for select_column in select_columns %}
  {% if select_column == 'segment_region_grouped' %}

  UNION

  SELECT *
  FROM large_subtotal

  {% endif %}
{% endfor %}

{% if select_columns[0] == 'segment_region_grouped' and select_columns|count > 1 %}

  UNION

  SELECT *
  FROM large_subtotal_extra

{% endif %}

)

SELECT
  fiscal_quarter_name_fy,
  {% for select_column in select_columns %}
    {{select_column}},
  {% endfor %}
  "MQLs / A",
  "MQLs / QTD",
  "MQLs / A" / "MQLs / QTD"                   AS "MQLs / %QTD",
  "MQLs / TQ",
  "MQLs / A" / "MQLs / TQ"                    AS "MQLs / %TQ",

  "Trials / A",
  "Trials / QTD",
  "Trials / A" / "Trials / QTD"               AS "Trials / %QTD",
  "Trials / TQ",
  "Trials / A" / "Trials / TQ"                AS "Trials / %TQ",

  "First Order ARR / A" / "New Logos / A"     AS "ASP / A",
  "First Order ARR / TQ" / "New Logos / TQ"   AS "ASP / TQ",
  "ASP / A" / "ASP / TQ"                      AS "ASP / %TQ",

  "SAOs / A" / "MQLs / A"                     AS "MQLs to SAOs / A",
  "SAOs / TQ" / "MQLs / TQ"                   AS "MQLs to SAOs / TQ",
  "MQLs to SAOs / A" / "MQLs to SAOs / TQ"    AS "MQLs to SAOs / %TQ",

  "Won Opps / A" / "Total Opps / A"           AS "Win Rate / A",
  "Won Opps / TQ" / "Total Opps / TQ"         AS "Win Rate / TQ",
  "Win Rate / A" / "Win Rate / TQ"            AS "Win Rate / %TQ"

FROM final

{%- endmacro -%}
