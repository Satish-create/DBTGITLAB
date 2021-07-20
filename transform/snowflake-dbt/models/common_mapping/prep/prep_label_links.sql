{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_labels', 'prep_labels')
]) }}

, gitlab_dotcom_label_links_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source')}}

), renamed AS (
  
    SELECT
      gitlab_dotcom_label_links_source.label_link_id     AS dim_label_link_id,
      -- FOREIGN KEYS
      gitlab_dotcom_label_links_source.label_id         as dim_label_id,
      -- foreign key to different table depending on target type of label
      CASE
        WHEN gitlab_dotcom_label_links_source.target_type = 'Issue' 
        THEN gitlab_dotcom_label_links_source.target_id
        ELSE NULL
      END AS dim_issue_id,
      CASE
        WHEN gitlab_dotcom_label_links_source.target_type = 'MergeRequest' 
        THEN gitlab_dotcom_label_links_source.target_id
        ELSE NULL
      END AS dim_merge_request_id,
      CASE
        WHEN gitlab_dotcom_label_links_source.target_type = 'Epic' 
        THEN gitlab_dotcom_label_links_source.target_id
        ELSE NULL
      END AS dim_epic_id,
      --
      gitlab_dotcom_label_links_source.target_type,
      gitlab_dotcom_label_links_source.label_link_created_at,
      gitlab_dotcom_label_links_source.label_link_updated_at,
      gitlab_dotcom_label_links_source.valid_from
      --

    FROM gitlab_dotcom_label_links_source
     LEFT JOIN prep_labels ON gitlab_dotcom_label_links_source.label_id = prep_labels.dim_label_id

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-07-15",
    updated_date="2021-07-15"
) }}