WITH mart_arr AS (

    SELECT *
    FROM {{ ref('arr_data_mart') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), base AS (

    SELECT DISTINCT
      date_actual                       AS arr_month,
      ultimate_parent_account_name,
      ultimate_parent_account_id
    FROM mart_arr
    CROSS JOIN dim_dates
    WHERE day_of_month = 1
      AND date_actual < DATE_TRUNC('month',CURRENT_DATE)
    ORDER BY 2, 1 DESC

), quarterly_arr_parent_level AS (

    SELECT
      base.arr_month                                                                         AS arr_quarter,
      base.ultimate_parent_account_name,
      base.ultimate_parent_account_id,
      ARRAY_AGG(DISTINCT product_category) WITHIN GROUP (ORDER BY product_category ASC)      AS product_category,
      ARRAY_AGG(DISTINCT delivery) WITHIN GROUP (ORDER BY delivery ASC)                      AS delivery,
      MAX(DECODE(product_category,   --Need to account for the 'other' categories
          'Bronze', 1,
          'Silver', 2,
          'Gold', 3,

          'Starter', 1,
          'Premium', 2,
          'Ultimate', 3,
          0
     ))                                                                                       AS product_ranking,
      SUM(ZEROIFNULL(quantity))                                                               AS quantity,
      SUM(ZEROIFNULL(mrr)*12)                                                                 AS arr
    FROM base
    LEFT JOIN mart_arr
      ON base.arr_month = mart_arr.arr_month
      AND base.ultimate_parent_account_id = mart_arr.ultimate_parent_account_id
    INNER JOIN dim_dates
      ON base.arr_month = dim_dates.date_actual
    WHERE base.arr_month = date_trunc('month', last_day_of_fiscal_quarter)
    {{ dbt_utils.group_by(n=3) }}

), prior_quarter AS (

    SELECT
      quarterly_arr_parent_level.*,
      LAG(product_category) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_quarter) AS previous_quarter_product_category,
      LAG(delivery) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_quarter) AS previous_quarter_delivery,
      COALESCE(LAG(product_ranking) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_quarter),0) AS previous_quarter_product_ranking,
      COALESCE(LAG(quantity) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_quarter),0) AS previous_quarter_quantity,
      COALESCE(LAG(arr) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_quarter),0) AS previous_quarter_arr
    FROM quarterly_arr_parent_level

), type_of_arr_change AS (

    SELECT
      prior_quarter.*,
      CASE
        WHEN previous_quarter_arr = 0 AND arr > 0
          THEN 'New'
        WHEN arr = 0 AND previous_quarter_arr > 0
          THEN 'Churn'
	    WHEN arr < previous_quarter_arr AND arr > 0
          THEN 'Contraction'
	    WHEN arr > previous_quarter_arr
          THEN 'Expansion'
	    WHEN arr = previous_quarter_arr
          THEN 'No Impact'
	    ELSE NULL
	  END                 AS type_of_arr_change
    FROM prior_quarter

), reason_for_arr_change_beg AS (

    SELECT
      arr_quarter,
      ultimate_parent_account_id,
      previous_quarter_arr      AS beg_arr,
      previous_quarter_quantity AS beg_quantity
    FROM type_of_arr_change

), reason_for_arr_change_seat_change AS (

    SELECT
      arr_quarter,
      ultimate_parent_account_id,
      CASE
        WHEN previous_quarter_quantity != quantity AND previous_quarter_quantity > 0
          THEN ZEROIFNULL(previous_quarter_arr/NULLIF(previous_quarter_quantity,0) * (quantity - previous_quarter_quantity))
        WHEN previous_quarter_quantity != quantity AND previous_quarter_quantity = 0
          THEN arr
        ELSE 0
      END                AS seat_change_arr,
      CASE
        WHEN previous_quarter_quantity != quantity
        THEN quantity - previous_quarter_quantity
        ELSE 0
      END                AS seat_change_quantity
    FROM type_of_arr_change

), reason_for_arr_change_price_change AS (

    SELECT
      arr_quarter,
      ultimate_parent_account_id,
      CASE
        WHEN previous_quarter_product_category = product_category
          THEN quantity * (arr/NULLIF(quantity,0) - previous_quarter_arr/NULLIF(previous_quarter_quantity,0))
        WHEN previous_quarter_product_category != product_category AND previous_quarter_product_ranking = product_ranking
          THEN quantity * (arr/NULLIF(quantity,0) - previous_quarter_arr/NULLIF(previous_quarter_quantity,0))
        ELSE 0
      END                  AS price_change_arr
    FROM type_of_arr_change

), reason_for_arr_change_tier_change AS (

    SELECT
      arr_quarter,
      ultimate_parent_account_id,
      CASE
        WHEN previous_quarter_product_ranking != product_ranking
        THEN ZEROIFNULL(quantity * (arr/NULLIF(quantity,0) - previous_quarter_arr/NULLIF(previous_quarter_quantity,0)))
        ELSE 0
      END                   AS tier_change_arr
    FROM type_of_arr_change

), reason_for_arr_change_end AS (

    SELECT
      arr_quarter,
      ultimate_parent_account_id,
      arr                   AS end_arr,
      quantity              AS end_quantity
    FROM type_of_arr_change

), annual_price_per_seat_change AS (

    SELECT
      arr_quarter,
      ultimate_parent_account_id,
      ZEROIFNULL(( arr / NULLIF(quantity,0) ) - ( previous_quarter_arr / NULLIF(previous_quarter_quantity,0))) AS annual_price_per_seat_change
    FROM type_of_arr_change

), combined AS (

    SELECT
      {{ dbt_utils.surrogate_key(['type_of_arr_change.arr_quarter', 'type_of_arr_change.ultimate_parent_account_id']) }} AS primary_key,
      type_of_arr_change.arr_quarter,
      type_of_arr_change.ultimate_parent_account_name,
      type_of_arr_change.ultimate_parent_account_id,
      type_of_arr_change.product_category,
      type_of_arr_change.previous_quarter_product_category,
      type_of_arr_change.delivery,
      type_of_arr_change.previous_quarter_delivery,
      type_of_arr_change.product_ranking,
      type_of_arr_change.previous_quarter_product_ranking,
      type_of_arr_change.type_of_arr_change,
      reason_for_arr_change_beg.beg_arr,
      reason_for_arr_change_beg.beg_quantity,
      reason_for_arr_change_seat_change.seat_change_arr,
      reason_for_arr_change_seat_change.seat_change_quantity,
      reason_for_arr_change_price_change.price_change_arr,
      reason_for_arr_change_tier_change.tier_change_arr,
      reason_for_arr_change_end.end_arr,
      reason_for_arr_change_end.end_quantity,
      annual_price_per_seat_change.annual_price_per_seat_change
    FROM type_of_arr_change
    LEFT JOIN reason_for_arr_change_beg
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_beg.ultimate_parent_account_id
      AND type_of_arr_change.arr_quarter = reason_for_arr_change_beg.arr_quarter
    LEFT JOIN reason_for_arr_change_seat_change
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_seat_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_quarter = reason_for_arr_change_seat_change.arr_quarter
    LEFT JOIN reason_for_arr_change_price_change
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_price_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_quarter = reason_for_arr_change_price_change.arr_quarter
    LEFT JOIN reason_for_arr_change_tier_change
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_tier_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_quarter = reason_for_arr_change_tier_change.arr_quarter
    LEFT JOIN reason_for_arr_change_end
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_end.ultimate_parent_account_id
      AND type_of_arr_change.arr_quarter = reason_for_arr_change_end.arr_quarter
    LEFT JOIN annual_price_per_seat_change
      ON type_of_arr_change.ultimate_parent_account_id = annual_price_per_seat_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_quarter = annual_price_per_seat_change.arr_quarter
    WHERE type_of_arr_change.arr_quarter < DATE_TRUNC('month',CURRENT_DATE)

)

SELECT *
FROM combined
