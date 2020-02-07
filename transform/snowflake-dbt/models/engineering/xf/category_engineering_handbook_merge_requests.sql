WITH diff_id_broken_out AS (
    
    SELECT 
      {{ dbt_utils.star(from=ref('engineering_specific_handbook_merge_requests'), except=['merge_request_version_url_path']) }},
      SPLIT(merge_request_version_url_path, 'diff_id=')[1]::BIGINT AS DIFF_ID
    FROM {{ ref('engineering', 'engineering_specific_handbook_merge_requests') }}

), joined_to_mr AS (

    SELECT 
      mr.merge_request_state,
      mr.updated_at,
      di.*
    FROM 
      diff_id_broken_out di
      JOIN
      {{ ref('gitlab_dotcom', 'gitlab_dotcom_merge_request_diffs')}} diff
      ON (diff.merge_request_diff_id = di.DIFF_ID)
      JOIN
      {{ ref('gitlab_dotcom', 'gitlab_dotcom_merge_requests')}} mr
      ON (diff.merge_request_id = mr.merge_request_id)

)
SELECT * FROM joined_to_mr