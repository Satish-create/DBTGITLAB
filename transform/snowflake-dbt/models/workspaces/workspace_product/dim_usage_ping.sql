
{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
    })
}}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, source AS (

    SELECT top 1000
      id                                                                        AS dim_service_ping_id,
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *,
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash
    FROM {{ ref('version_usage_data_source') }}

), raw_usage_data AS (

    SELECT *
    FROM {{ ref('version_raw_usage_data_source') }}

), map_ip_to_country AS (

    SELECT *
    FROM {{ ref('map_ip_to_country') }}

), locations AS (

    SELECT *
    FROM {{ ref('prep_location_country') }}

), usage_data AS (

    SELECT
      host_id                                                                                                       AS dim_host_id,
      uuid                                                                                                          AS dim_instance_id,
      source.*,
      edition                                                                                                       AS original_edition,
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, edition, 'EE Free')                  AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                               AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                                       AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                                   AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                                   AS minor_version,
      major_version || '.' || minor_version                                                                         AS major_minor_version
    FROM source
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%')

), joined_ping AS (

    SELECT
      usage_data.*,
      cleaned_edition                                                                           AS edition,
      IFF(original_edition = 'CE', 'CE', 'EE')                                                  AS main_edition,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                     AS ping_source,
      CASE
        WHEN ping_source = 'SaaS'                               THEN TRUE
        WHEN installation_type = 'gitlab-development-kit'       THEN TRUE
        WHEN hostname = 'gitlab.com'                            THEN TRUE
        WHEN hostname ILIKE '%.gitlab.com'                      THEN TRUE
        ELSE FALSE END                                                                          AS is_internal,
      CASE
        WHEN hostname ilike 'staging.%'                         THEN TRUE
        WHEN hostname IN (
        'staging.gitlab.com',
        'dr.gitlab.com'
      )                                                         THEN TRUE
        ELSE FALSE END                                                                          AS is_staging,
        hostname                                                                                AS host_name,
      COALESCE(raw_usage_data.raw_usage_data_payload, usage_data.raw_usage_data_payload_reconstructed)     AS raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id

), map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash,
      map_ip_to_country.dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), add_country_info_to_usage_ping AS (

    SELECT
      joined_ping.*,
      map_ip_location.dim_location_country_id
    FROM joined_ping
    LEFT JOIN map_ip_location
      ON joined_ping.ip_address_hash = map_ip_location.ip_address_hash

), dim_product_tier AS (

  SELECT *
  FROM {{ ref('dim_product_tier') }}
  WHERE product_delivery_type = 'Self-Managed'

), prep_usage_ping_cte AS (

    SELECT
      add_country_info_to_usage_ping.*,
      ping_source                                       AS service_ping_delivery_type
    FROM add_country_info_to_usage_ping

), joined_payload AS (

    SELECT
      prep_usage_ping_cte.*,
      prep_license.dim_license_id,
      prep_subscription.dim_subscription_id,
      dim_date.date_id,
      TO_DATE(raw_usage_data.raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (raw_usage_data.raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      raw_usage_data.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value,
      IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)                                       AS is_trial
    FROM prep_usage_ping_cte
    LEFT JOIN raw_usage_data
      ON prep_usage_ping_cte.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    LEFT JOIN prep_license
      ON prep_usage_ping_cte.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date
      ON TO_DATE(ping_created_at) = dim_date.date_day

), dim_product_tier AS (

    SELECT *
    FROM {{ ref('dim_product_tier') }}
    WHERE product_delivery_type = 'Self-Managed'

), final AS (

    SELECT
      joined_payload.*,
      dim_product_tier.dim_product_tier_id                   AS dim_product_tier_id,
      COALESCE(license_subscription_id, dim_subscription_id) AS dim_subscription_id,
      date_id                                                AS dim_date_id,
      DATEADD('days', -28, ping_created_at)                  AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                    AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)                   AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                    AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                     AS ping_created_at_date
    FROM joined_payload
    LEFT JOIN dim_product_tier
      ON TRIM(LOWER(joined_payload.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
      AND edition = 'EE'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@@icooper-acp",
    updated_by="@@icooper-acp",
    created_date="2022-03-08",
    updated_date="2022-03-08"
) }}
