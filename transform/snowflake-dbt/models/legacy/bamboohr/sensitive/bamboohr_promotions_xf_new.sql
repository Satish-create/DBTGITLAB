WITH pay_frequency_table AS (
  SELECT
    locality,
    pay_frequency,
    COUNT(*) AS frequancy
  FROM prep.bamboohr.bamboohr_id_employee_number_mapping_source
  WHERE TRUE
    AND pay_frequency IS NOT NULL
  GROUP BY 1, 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY locality ORDER BY frequancy DESC ) = 1
  ORDER BY 1, 2
),
  mapping_source AS (
    SELECT
      employee_number,
      employee_id,
      hire_date,
      source.pay_frequency,
      LAST_VALUE(source.pay_frequency IGNORE NULLS)
                 OVER (PARTITION BY employee_id ORDER BY uploaded_at) AS last_pay_frequency,
      FIRST_VALUE(source.uploaded_at IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY uploaded_at) AS first_upload,
      COALESCE(source.pay_frequency, pay_frequency_table.pay_frequency, last_pay_frequency) AS calc_pay_frequency,
      uploaded_at
    FROM prep.bamboohr.bamboohr_id_employee_number_mapping_source source
    LEFT JOIN pay_frequency_table
      ON source.locality = pay_frequency_table.locality
      --WHERE employee_id = 42513
    ORDER BY uploaded_at
  ),
  mapping_gropus AS (
    SELECT
      employee_number,
      employee_id,
      hire_date,
      first_upload,
      calc_pay_frequency,
      LAG(calc_pay_frequency, 1, '') OVER (PARTITION BY employee_number ORDER BY uploaded_at) AS lag_pay_frequency,
      CONDITIONAL_TRUE_EVENT(calc_pay_frequency != lag_pay_frequency)
                             OVER ( PARTITION BY employee_number ORDER BY uploaded_at) AS pay_frequency_group,
      uploaded_at
    FROM mapping_source
  ),
  mapping AS (
    SELECT
      employee_number,
      employee_id,
      hire_date,
      calc_pay_frequency AS pay_frequency,
      MIN(IFF(uploaded_at = first_upload AND first_upload > hire_date, hire_date, uploaded_at)) AS valid_from,
      MAX(uploaded_at) AS valid_to
    FROM mapping_gropus
    GROUP BY employee_number, employee_id, hire_date, pay_frequency, pay_frequency_group
  ),


  conversion AS (
    SELECT
      *,
      LEAD(DATEADD(DAY, -1, effective_date), 1, CURRENT_DATE())
           OVER (PARTITION BY employee_id ORDER BY conversion_id) AS next_effective_date
    FROM prep.bamboohr.bamboohr_currency_conversion_source
    --WHERE employee_id = 41433
  ),
  bamboohr_on_target_earnings_source AS (
    SELECT
      *
    FROM prep.bamboohr.bamboohr_ote_source
      QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY target_earnings_update_id DESC) = 1
  ),
  bamboohr_compensation_source AS (
    SELECT
      *
    FROM prep.bamboohr.bamboohr_compensation_source
      QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY compensation_update_id DESC) = 1
  ),
  workday_on_target_earnings_source AS (
    SELECT
      *
    FROM prep.workday.workday_on_target_earnings_source
      QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY effective_date) =
              1 -- need initiated at
  ),
  workday_compensation_source AS (
    SELECT
      *
    FROM prep.workday.workday_compensation_source
      QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id,effective_date ORDER BY initiated_at DESC) = 1
  ),
  employee_directory_source AS (
    SELECT
      *
    FROM "PREP".sensitive.employee_directory_intermediate
  ),
  employee_directory AS (
    SELECT
      employee_directory_source.full_name,
      employee_directory_source.division_mapped_current,
      employee_directory_source.division_grouping,
      employee_directory_source.department_modified,
      employee_directory_source.department_grouping,
      employee_directory_source.job_title,
      employee_directory_source.date_actual,
      employee_directory_source.employee_id,
      employee_directory_source.employee_number
    FROM employee_directory_source
  ),

  bamboohr_on_target_earnings AS (
    SELECT
      mapping.employee_number AS employee_id,
      --bamboohr_on_target_earnings_source.employee_id as old_employee_id,
      bamboohr_on_target_earnings_source.effective_date,
      --on_target_earnings_source.variable_pay,
      bamboohr_on_target_earnings_source.annual_amount_usd_value,
      bamboohr_on_target_earnings_source.prior_annual_amount_usd,
      bamboohr_on_target_earnings_source.change_in_annual_amount_usd
    FROM bamboohr_on_target_earnings_source
    LEFT JOIN mapping
      ON bamboohr_on_target_earnings_source.employee_id = mapping.employee_id
      AND bamboohr_on_target_earnings_source.effective_date BETWEEN mapping.valid_from AND mapping.valid_to
    WHERE effective_date <= CURRENT_DATE()
  ),

  --select * from bamboohr_on_target_earnings_source where old_employee_id = 41117;


  workday_on_target_earnings AS (
    SELECT
      employee_id,
      effective_date,
      annual_amount_usd_value,
      LAG(annual_amount_usd_value)
          OVER (PARTITION BY employee_id ORDER BY effective_date) AS prior_annual_amount_usd, -- need initiated at
      annual_amount_usd_value - prior_annual_amount_usd AS change_in_annual_amount_usd
    FROM workday_on_target_earnings_source
  ),
  blended_on_target_earnings AS (
    SELECT
      *
    FROM bamboohr_on_target_earnings

    UNION ALL

    SELECT
      *
    FROM workday_on_target_earnings
  ),
  bamboohr_compensation AS (
    SELECT
      mapping.employee_number AS new_employee_id,
      --compensation.employee_id,
      bamboohr_compensation_source.effective_date,
      bamboohr_compensation_source.compensation_change_reason,
      bamboohr_compensation_source.pay_rate,
      --bamboohr_compensation_source.compensation_value,
      --mapping.pay_frequency,
      ROUND(bamboohr_compensation_source.compensation_value * mapping.pay_frequency, 2) AS annual_compensation_value,
      bamboohr_compensation_source.compensation_currency,
      conversion.currency_conversion_factor AS conversion_rate_local_to_usd,
      conversion.annual_local_usd_code AS compensation_currency_usd,
      ROUND(annual_compensation_value * conversion_rate_local_to_usd, 2) AS compensation_value_usd
    FROM bamboohr_compensation_source
    LEFT JOIN mapping
      ON bamboohr_compensation_source.employee_id = mapping.employee_id
      AND bamboohr_compensation_source.effective_date BETWEEN mapping.valid_from AND mapping.valid_to
    LEFT JOIN conversion
      ON bamboohr_compensation_source.employee_id = conversion.employee_id
      AND
         bamboohr_compensation_source.effective_date BETWEEN conversion.effective_date AND conversion.next_effective_date
  ),

  workday_compensation AS (
    SELECT
      employee_id,
      --uploaded_at,
      effective_date,
      --compensation_type,
      compensation_change_reason,
      pay_rate,
      compensation_value,
      compensation_currency,
      conversion_rate_local_to_usd,
      compensation_currency_usd,
      compensation_value_usd
      --initiated_at
    FROM workday_compensation_source
  ),
  blended_compensation AS (
    SELECT
      *
    FROM bamboohr_compensation

    UNION ALL

    SELECT
      *
    FROM workday_compensation
  ),

  compensation_changes AS (
    SELECT
      blended_compensation.new_employee_id AS employee_id,
      blended_compensation.effective_date,
      blended_compensation.compensation_change_reason,
      blended_compensation.pay_rate,
      blended_compensation.annual_compensation_value,
      blended_compensation.compensation_currency,
      blended_compensation.conversion_rate_local_to_usd,
      blended_compensation.compensation_currency_usd,
      blended_compensation.compensation_value_usd,
      LAG(blended_compensation.compensation_currency)
          OVER (PARTITION BY blended_compensation.new_employee_id ORDER BY blended_compensation.effective_date ASC) AS prior_compensation_currency,
      LAG(blended_compensation.conversion_rate_local_to_usd)
          OVER (PARTITION BY blended_compensation.new_employee_id ORDER BY blended_compensation.effective_date ASC) AS prior_conversion_rate_local_to_usd,
      LAG(blended_compensation.compensation_value_usd)
          OVER (PARTITION BY blended_compensation.new_employee_id ORDER BY blended_compensation.effective_date ASC) AS prior_compensation_value_usd,
      COALESCE(blended_on_target_earnings.annual_amount_usd_value, 0) AS ote_usd,
      COALESCE(blended_on_target_earnings.prior_annual_amount_usd, 0) AS prior_ote_usd,
      COALESCE(blended_on_target_earnings.change_in_annual_amount_usd, 0) AS ote_change,
      IFF(blended_on_target_earnings.employee_id IS NOT NULL, 'Yes', 'No') AS variable_pay
    FROM blended_compensation
    LEFT JOIN blended_on_target_earnings
      ON blended_compensation.new_employee_id = blended_on_target_earnings.employee_id
      AND blended_compensation.effective_date = blended_on_target_earnings.effective_date
  )


SELECT
  'TBD' AS compensation_update_id,
  compensation_changes.effective_date AS promotion_date,
  DATE_TRUNC('month', compensation_changes.effective_date) AS promotion_month,
  compensation_changes.employee_id,
  compensation_changes.employee_id,
  employee_directory.full_name AS full_name,
  employee_directory.division_mapped_current AS division,
  employee_directory.division_grouping AS division_grouping,
  employee_directory.department_modified AS department,
  employee_directory.department_grouping AS department_grouping,
  employee_directory.job_title AS job_title,
  variable_pay AS variable_pay,
  --compensation_change_reason,
  --pay_rate,
  --_compensation_value,
  compensation_currency,
  prior_compensation_currency,
  conversion_rate_local_to_usd,
  prior_conversion_rate_local_to_usd,
  compensation_currency_usd,
  compensation_value_usd AS new_compensation_value_usd,
  prior_compensation_value_usd,
  --IFF(compensation_currency = prior_compensation_currency,conversion_rate_local_to_usd / prior_conversion_rate_local_to_usd,1 ) AS adjust_prior_to_current,
  --prior_compensation_value_usd * adjust_prior_to_current AS adjusted_prior_compensation_value_usd,
  --compensation_value_usd - adjusted_prior_compensation_value_usd AS change_in_comp_usd,
  ote_usd,
  prior_ote_usd,
  ote_change--,
  --adjusted_prior_compensation_value_usd + prior_ote_usd AS previous_total_compensation,
  --change_in_comp_usd + ote_change AS total_change_in_comp,
  --ROUND(total_change_in_comp / previous_total_compensation,2) AS percent_change_in_comp
FROM compensation_changes
LEFT JOIN employee_directory
  ON compensation_changes.employee_id = employee_directory.employee_number
  AND compensation_changes.effective_date = employee_directory.date_actual
WHERE TRUE
  --AND compensation_changes.employee_id = 11404
  --and prior_conversion_rate_local_to_usd = 0
  AND compensation_change_reason = 'Promotion'
ORDER BY effective_date