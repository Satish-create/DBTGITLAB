WITH
namespaces AS (
  SELECT *
  FROM prod.common.dim_namespace  -- prod.legacy.gitlab_dotcom_namespaces_xf
),

memberships AS (
  SELECT *
  FROM prod.legacy.gitlab_dotcom_memberships
),

users AS (
  SELECT *
  FROM prod.common.dim_user
),

crm_accounts AS (
  SELECT *
  FROM prod.restricted_safe_common.dim_crm_account
),

crm_persons AS (
  SELECT *
  FROM prod.common.dim_crm_person
),

companies AS (
  SELECT *
  FROM prod.workspace_marketing.wk_dim_company
),

marketing_contacts AS (
  SELECT *
  FROM prod.common.dim_marketing_contact_no_pii
),

user_company_bridge AS (
  SELECT *
  FROM prod.workspace_marketing.wk_bdg_user_company
),

customer_charges AS (
  SELECT *
  FROM prod.restricted_safe_legacy.customers_db_charges_xf
),

mart_arr AS (
  SELECT *
  FROM prod.restricted_safe_common_mart_sales.mart_arr
),


  namespace_email_domain AS (
    SELECT
      namespaces.dim_namespace_id AS namespace_id,
      users.email_domain,
      COUNT(DISTINCT memberships.user_id) AS number_of_users,
      ROW_NUMBER() OVER (PARTITION BY namespaces.dim_namespace_id ORDER BY number_of_users DESC) AS row_number
    FROM namespaces
    INNER JOIN memberships
      ON memberships.ultimate_parent_id = namespaces.dim_namespace_id AND memberships.is_billable = TRUE
    INNER JOIN users
      ON users.dim_user_id = memberships.user_id
    WHERE namespaces.namespace_is_ultimate_parent = TRUE
      AND users.email_domain_classification IS NULL
    GROUP BY 1, 2
  ),

salseforce_company_id AS (
    SELECT
      crm_accounts.dim_crm_account_id,
      crm_accounts.dim_parent_crm_account_id,
      crm_accounts.crm_account_name,
      crm_accounts.parent_crm_account_name,
      companies.company_id,
      companies.source_company_id
    FROM crm_accounts
    LEFT JOIN companies
      ON crm_accounts.crm_account_zoom_info_dozisf_zi_id = companies.company_id -- this joins 2K more company ids than merged_zid, so using this for now
    -- left join merged_zid zc   on c.CRM_ACCOUNT_ZOOM_INFO_DOZISF_ZI_ID = zc.DOZISF_ZI_ID
  ),

account_email_domain AS (
    SELECT
      crm_accounts.dim_crm_account_id,
      crm_persons.email_domain,
      crm_persons.email_domain_type AS email_domain_flag,
      CASE
        WHEN crm_persons.email_domain_type = 'Business email domain' THEN 1
        ELSE 0
      END AS business_email,
      COUNT(*) AS no_ids,
      SUM(no_ids) OVER (PARTITION BY crm_accounts.dim_crm_account_id) AS total_ids,
      ROW_NUMBER() OVER (PARTITION BY crm_accounts.dim_crm_account_id ORDER BY business_email DESC, no_ids DESC) AS rn,
      ROUND(no_ids / total_ids, 4) AS ratio_of_users
    FROM crm_accounts
    LEFT JOIN crm_persons
      ON crm_persons.dim_crm_account_id = crm_accounts.dim_crm_account_id
    GROUP BY 1, 2, 3, 4
  ),

 user_account AS (
    SELECT DISTINCT
      users.dim_user_id AS user_id,
      users.email_domain,
      users.email_domain_classification,
      marketing_contacts.company_name,
      marketing_contacts.dim_crm_account_id,
      crm_accounts.dim_parent_crm_account_id,
      IFF(account_email_domain.business_email = 1, salseforce_company_id.dim_crm_account_id, NULL) AS account_id, -- to reduce mapping to wrong account. reduces error rate from 10% to 5% and eliminated 20% of users having account_id
      IFF(account_email_domain.business_email = 1, salseforce_company_id.dim_parent_crm_account_id, NULL) AS parent_account_id,
      CASE
        WHEN marketing_contacts.dim_crm_account_id = account_id AND account_id IS NOT NULL THEN 1
        ELSE 0
      END AS account_match,
      ROW_NUMBER() OVER (PARTITION BY users.dim_user_id ORDER BY COALESCE(account_email_domain.rn, 10) ASC ,account_email_domain.no_ids DESC ) AS row_number
    FROM users
    INNER JOIN marketing_contacts
      ON users.dim_user_id = marketing_contacts.gitlab_dotcom_user_id
    LEFT JOIN crm_accounts
      ON crm_accounts.dim_crm_account_id = marketing_contacts.dim_crm_account_id
    LEFT JOIN user_company_bridge
      ON users.dim_user_id = user_company_bridge.gitlab_dotcom_user_id -- only pulls the product user, because we join to dim_user

    LEFT JOIN salseforce_company_id
      ON salseforce_company_id.company_id = user_company_bridge.company_id
      --left join doc_zid d2 on d2.company_id = c.company_id
    LEFT JOIN account_email_domain
      ON account_email_domain.dim_crm_account_id = salseforce_company_id.dim_crm_account_id
           AND users.email_domain = account_email_domain.email_domain
           AND account_email_domain.rn < 4
           AND account_email_domain.business_email = 1
    --  and edom.dim_crm_account_id = d2.ACCOUNT_ID --: to restrict only to company with a single account id. It will improve accuract rate from 85%/90% at account/parent account level to 95/96%
   QUALIFY row_number = 1
  ),

namespace_subscription AS (
  SELECT
    ultimate_parent_namespace_id AS namespace_id,
    namespace_type,
    subscription_name_slugify,
    MIN(subscription_start_date) AS sd
  FROM customer_charges
  INNER JOIN namespaces
    ON customer_charges.current_gitlab_namespace_id::INT = namespaces.dim_namespace_id
  WHERE customer_charges.current_gitlab_namespace_id IS NOT NULL
    --and ch.product_category IN ('SaaS - Ultimate','SaaS - Premium','SaaS - Bronze')
  GROUP BY 1, 2, 3
),

mrr_namespaces AS (
    SELECT
      namespace_subscription.namespace_id,
      namespace_type,
      salseforce_company_id.dim_parent_crm_account_id,
      salseforce_company_id.dim_crm_account_id,
      salseforce_company_id.company_id,
      salseforce_company_id.source_company_id,
      MAX(arr_month) AS max_arr_month,
      ROW_NUMBER() OVER (PARTITION BY namespace_subscription.namespace_id ORDER BY max_arr_month DESC) AS rn
    FROM namespace_subscription
    INNER JOIN mart_arr
      ON namespace_subscription.subscription_name_slugify = mart_arr.subscription_name_slugify AND mart_arr.product_delivery_type = 'SaaS'
      -- and m.product_rate_plan_name != 'Gitlab Storage 10GB'
      AND arr_month < getdate()::DATE
    LEFT JOIN salseforce_company_id
      ON salseforce_company_id.dim_crm_account_id = mart_arr.dim_crm_account_id
    GROUP BY 1, 2, 3, 4, 5, 6
  ),

 namespace_account_pre AS (
    SELECT
      namespaces.namespace_type,
      namespaces.dim_namespace_id AS namespace_id,
      memberships.user_id,
      user_account.email_domain,
      user_account.email_domain_classification,
      COALESCE(user_account.dim_crm_account_id, user_account.account_id) AS account_id,
      IFF(user_account.dim_crm_account_id IS NOT NULL, 1, 0) AS is_direct_account_mapping,
      IFF(namespaces.creator_id = memberships.user_id, TRUE, FALSE) AS is_creator,
      IFF(access_level = 50, TRUE, FALSE) AS is_owner
    FROM namespaces
    INNER JOIN memberships
      ON memberships.ultimate_parent_id = namespaces.dim_namespace_id AND memberships.is_billable = TRUE
    LEFT JOIN user_account
      ON user_account.user_id = memberships.user_id
    WHERE namespaces.namespace_is_ultimate_parent = TRUE
      QUALIFY ROW_NUMBER() OVER (PARTITION BY namespaces.dim_namespace_id,memberships.user_id ORDER BY access_level DESC) = 1
  ),

 namespace_account AS (
    SELECT
      namespace_type,
      namespace_id,
      account_id,
      CASE
        WHEN account_id IS NOT NULL THEN 1
        ELSE 0
      END AS has_account,                                                                                                          -- namepace and account level detail
      MAX(IFF(is_creator, 1, 0)) AS has_creator,
      MAX(CASE
            WHEN is_creator = TRUE AND is_direct_account_mapping = 1 THEN 1
            ELSE 0
          END) AS is_creator_direct_match,
      COUNT(DISTINCT IFF(is_owner, user_id, NULL)) AS total_owners,
      COUNT(DISTINCT user_id) AS total_users,                                                                                      -- overall namespace level
      COUNT(DISTINCT account_id) OVER (PARTITION BY namespace_id) AS total_num_accounts,
      SUM(total_owners) OVER (PARTITION BY namespace_id) AS total_namespace_owners,
      SUM(total_users) OVER (PARTITION BY namespace_id) AS total_namespace_users,                                                  -- overall namespace level but only when account id is present
      SUM(IFF(account_id IS NOT NULL, total_owners, 0))
          OVER (PARTITION BY namespace_id) AS total_namespace_owners_qualified,
      SUM(IFF(account_id IS NOT NULL, total_users, 0))
          OVER (PARTITION BY namespace_id) AS total_namespace_users_qualified,                                                     -- row number
      ROW_NUMBER() OVER ( PARTITION BY namespace_id ORDER BY has_account DESC, has_creator DESC, total_owners DESC,total_users DESC) AS rn,
      ROW_NUMBER() OVER ( PARTITION BY namespace_id ORDER BY has_account DESC, total_owners DESC,total_users DESC) AS rn_owner,
      ROW_NUMBER() OVER ( PARTITION BY namespace_id ORDER BY has_account DESC,total_users DESC) AS rn_user,                        --Nth value
      NTH_VALUE(total_owners, 1)
                OVER (PARTITION BY namespace_id ORDER BY has_account DESC, total_owners DESC,total_users DESC) AS owner_count_1st,
      NTH_VALUE(total_owners, 2)
                OVER (PARTITION BY namespace_id ORDER BY has_account DESC, total_owners DESC,total_users DESC) AS owner_count_2nd, --pulls the count of the 2nd most
      NTH_VALUE(total_users, 1)
                OVER (PARTITION BY namespace_id ORDER BY has_account DESC,total_users DESC) AS users_count_1st,
      NTH_VALUE(total_users, 2)
                OVER (PARTITION BY namespace_id ORDER BY has_account DESC,total_users DESC) AS users_count_2nd,
      NTH_VALUE(has_account, 2)
                OVER (PARTITION BY namespace_id ORDER BY has_account DESC,total_users DESC) AS users_count_2nd_has_account,        -- % calculations
      IFF(total_namespace_owners_qualified > 0, total_owners / total_namespace_owners_qualified,
          0) AS owner_percent_qualified,
      IFF(total_namespace_users_qualified > 0, total_users / total_namespace_users_qualified,
          0) AS users_percent_qualified,
      IFF(total_namespace_owners > 0, total_owners / total_namespace_owners, 0) AS owner_percent,
      IFF(total_namespace_users > 0, total_users / total_namespace_users, 0) AS users_percent
    FROM namespace_account_pre
    GROUP BY 1, 2, 3, 4
  ),

most_matched_namespace_account AS (
    SELECT
      namespace_account.namespace_id,
      namespace_account.users_percent_qualified,
      namespace_account.account_id,
      namespace_account.total_users
    FROM namespace_account
-- left join ns_account u2 on u2.namespace_id = u1.namespace_id and u2.has_account = 1 and u2.rn_user = 2
    WHERE namespace_account.rn_user = 1
      AND namespace_account.total_users >= 1
      AND namespace_account.has_account = 1
      AND (COALESCE(namespace_account.users_count_2nd_has_account, 0) = 0 OR
           (namespace_account.users_count_1st > COALESCE(namespace_account.users_count_2nd, 0) AND COALESCE(namespace_account.users_count_2nd_has_account, 1) = 1)
      )
  ),

company_email_domain AS (
    SELECT
      companies.source_company_id,
      users.email_domain_classification,
      users.email_domain,
      CASE
        WHEN users.email_domain_classification IS NULL THEN 1
        ELSE 0
      END AS business_email,
      COUNT(DISTINCT users.dim_user_id) AS no_users,
      ROW_NUMBER() OVER (PARTITION BY companies.source_company_id ORDER BY business_email DESC, no_users DESC) AS rn
    FROM user_company_bridge
    INNER JOIN users
      ON users.dim_user_id = user_company_bridge.gitlab_dotcom_user_id
    INNER JOIN companies
      ON companies.company_id = user_company_bridge.company_id
    WHERE users.email_domain_classification IS NULL -- only business emails
    GROUP BY 1, 2, 3
  ),

user_company AS (
    SELECT DISTINCT
      user_id,
      email_domain,
      email_domain_classification,
      source_company_id,
      IFF(edom.rn IS NOT NULL, 1, 0) AS top_company_emaildom -- to reduce mapping error .

FROM user_company_bridge
    INNER JOIN users
      ON users.dim_user_id = user_company_bridge.gitlab_dotcom_user_id
    INNER JOIN companies
      ON companies.company_id = user_company_bridge.company_id
    LEFT JOIN company_email_domain
      ON company_email_domain.source_company_id = companies.source_company_id
           AND users.email_domain = company_email_domain.email_domain
           AND company_email_domain.rn < 4
  )


     ,

namespace_company_pre AS (
    SELECT
      namespaces.namespace_type,
      namespaces.dim_namespace_id AS namespace_id,
      memberships.user_id,
      uc.email_domain,
      uc.email_domain_classification,
      uc.source_company_id,
      uc.top_company_emaildom,
      IFF(namespaces.creator_id = memberships.user_id, TRUE, FALSE) AS is_creator,
      IFF(access_level = 50, TRUE, FALSE) AS is_owner
    FROM namespaces
    INNER JOIN memberships
      ON memberships.ultimate_parent_id = namespaces.dim_namespace_id AND memberships.is_billable = TRUE
    LEFT JOIN user_company uc
      ON uc.user_id = memberships.user_id
    WHERE namespaces.namespace_is_ultimate_parent = TRUE
      QUALIFY ROW_NUMBER() OVER (PARTITION BY namespaces.dim_namespace_id,memberships.user_id ORDER BY access_level DESC) = 1
  ),

namespace_company AS (
    SELECT
      namespace_type,
      namespace_id,
      source_company_id,
      CASE
        WHEN source_company_id IS NOT NULL THEN 1
        ELSE 0
      END AS has_company,                                                                                                          -- namepace and account level detail
      MAX(IFF(is_creator, 1, 0)) AS has_creator,
      MAX(CASE
            WHEN is_creator = TRUE AND top_company_emaildom = 1 THEN 1
            ELSE 0
          END) AS is_creator_direct_match,
      COUNT(DISTINCT IFF(is_owner, user_id, NULL)) AS total_owners,
      COUNT(DISTINCT user_id) AS total_users,                                                                                      -- overall namespace level
      COUNT(DISTINCT source_company_id) OVER (PARTITION BY namespace_id) AS total_num_company,
      SUM(total_owners) OVER (PARTITION BY namespace_id) AS total_namespace_owners,
      SUM(total_users) OVER (PARTITION BY namespace_id) AS total_namespace_users,                                                  -- overall namespace level but only when company id is present
      SUM(IFF(source_company_id IS NOT NULL, total_owners, 0))
          OVER (PARTITION BY namespace_id) AS total_namespace_owners_qualified,
      SUM(IFF(source_company_id IS NOT NULL, total_users, 0))
          OVER (PARTITION BY namespace_id) AS total_namespace_users_qualified,
      SUM(IFF(source_company_id IS NOT NULL, has_creator, 0))
          OVER (PARTITION BY namespace_id) AS total_namespace_creator_qualified,                                                   -- row number
      ROW_NUMBER() OVER ( PARTITION BY namespace_id ORDER BY has_company DESC, has_creator DESC, total_owners DESC,total_users DESC) AS rn,
      ROW_NUMBER() OVER ( PARTITION BY namespace_id ORDER BY has_company DESC, total_owners DESC,total_users DESC) AS rn_owner,
      ROW_NUMBER() OVER ( PARTITION BY namespace_id ORDER BY has_company DESC,total_users DESC) AS rn_user,                        --Nth value
      NTH_VALUE(total_owners, 1)
                OVER (PARTITION BY namespace_id ORDER BY has_company DESC, total_owners DESC,total_users DESC) AS owner_count_1st,
      NTH_VALUE(total_owners, 2)
                OVER (PARTITION BY namespace_id ORDER BY has_company DESC, total_owners DESC,total_users DESC) AS owner_count_2nd, --pulls the count of the 2nd most
      NTH_VALUE(total_users, 1)
                OVER (PARTITION BY namespace_id ORDER BY has_company DESC,total_users DESC) AS users_count_1st,
      NTH_VALUE(total_users, 2)
                OVER (PARTITION BY namespace_id ORDER BY has_company DESC,total_users DESC) AS users_count_2nd,                    -- % calculations
      IFF(total_namespace_owners_qualified > 0, total_owners / total_namespace_owners_qualified,
          0) AS owner_percent_qualified,
      IFF(total_namespace_users_qualified > 0, total_users / total_namespace_users_qualified,
          0) AS users_percent_qualified,
      IFF(total_namespace_owners > 0, total_owners / total_namespace_owners, 0) AS owner_percent,
      IFF(total_namespace_users > 0, total_users / total_namespace_users, 0) AS users_percent
    FROM namespace_company_pre
    GROUP BY 1, 2, 3, 4
  ),

company_ids AS (
    SELECT
      source_company_id,
      MAX(salseforce_company_id.dim_crm_account_id) AS account_id,
      MAX(salseforce_company_id.dim_parent_crm_account_id) AS parent_account_id,
      COUNT(DISTINCT salseforce_company_id.dim_crm_account_id) AS no_a,
      COUNT(DISTINCT salseforce_company_id.dim_parent_crm_account_id) AS no_upa,
      ARRAY_TO_STRING(arrayagg(DISTINCT concat(salseforce_company_id.DIM_CRM_ACCOUNT_ID,':',salseforce_company_id.CRM_ACCOUNT_NAME) ), ',') AS list_of_accounts,
      ARRAY_TO_STRING(arrayagg(DISTINCT concat(salseforce_company_id.DIM_parent_CRM_ACCOUNT_ID,':',salseforce_company_id.parent_CRM_ACCOUNT_NAME) ), ',') AS list_of_upa
      -- , max(account_type_num ) as paid_status_num
    FROM salseforce_company_id
      --left join account_type a on a.DIM_CRM_ACCOUNT_ID = s.DIM_CRM_ACCOUNT_ID
    GROUP BY 1
  ),


 namespace_map AS
    (
      SELECT
        namespaces.namespace_type,
        namespaces.creator_id,
        namespaces.visibility_level,
        namespaces.dim_namespace_id AS namespace_id,
        namespaces.gitlab_plan_title,
        namespaces.gitlab_plan_is_paid,
        namespaces.is_setup_for_company,
        namespaces.created_at,
        CASE
          WHEN users.dim_user_id IS NULL THEN 'Missing'
          ELSE COALESCE(users.email_domain_classification, 'Business')
        END AS email_domain_type, --, up.setup_for_company
        mrr_namespaces.dim_crm_account_id AS actual_account_id,
        mrr_namespaces.source_company_id AS actual_company_id,
        IFF(namespace_company.namespace_id IS NOT NULL, namespace_company.source_company_id, NULL) AS predicted_company_id,
        IFF(namespace_account.namespace_id IS NOT NULL, namespace_account.account_id, NULL) AS predicted_account_id,
        COALESCE(actual_account_id, predicted_account_id) AS account_id,
        COALESCE(actual_company_id, predicted_company_id) AS company_id
      FROM namespaces
      LEFT JOIN users
        ON users.dim_user_id = namespaces.creator_id
      --LEFT JOIN user_preferances
        --ON user_preferances.user_id = users.dim_user_id
      LEFT JOIN mrr_namespaces
        ON mrr_namespaces.namespace_id = namespaces.dim_namespace_id AND mrr_namespaces.rn = 1
      LEFT JOIN namespace_company
        ON namespace_company.namespace_id = namespaces.dim_namespace_id
        AND namespace_company.rn = 1
        AND namespace_company.has_company = 1
        AND (
               (namespace_company.has_creator = 1 AND namespace_company.rn_owner = 1 AND namespace_company.rn_user = 1
                 AND namespace_company.owner_percent_qualified > 0.5 AND namespace_company.users_percent_qualified > 0.5
                 AND namespace_company.owner_percent > 0.2 AND namespace_company.users_percent > 0.2
                 )
               OR
               (namespace_company.has_creator = 0 AND namespace_company.rn_owner = 1 AND namespace_company.rn_user = 1 AND namespace_company.total_namespace_creator_qualified = 0
                 AND namespace_company.owner_percent_qualified > 0.6 AND namespace_company.users_percent_qualified > 0.6
                 AND namespace_company.owner_percent > 0.33 AND namespace_company.users_percent > 0.33
                 )
             )
      LEFT JOIN namespace_account
        ON namespace_account.namespace_id = namespaces.dim_namespace_id
        AND namespace_account.rn = 1
        AND has_account = 1
        AND (
               (namespace_account.has_creator = 1 AND namespace_account.rn_owner = 1 AND namespace_account.owner_percent_qualified > 0.5)
               OR
               (namespace_account.has_creator = 0 AND namespace_account.rn_owner = 1 AND namespace_account.owner_percent_qualified > 0.66)
             )
      WHERE namespaces.namespace_is_ultimate_parent = TRUE
        AND namespaces.namespace_is_internal = FALSE
        AND namespaces.visibility_level IN ('public', 'private')
        AND namespaces.gitlab_plan_title != 'Default'
        AND namespaces.namespace_creator_is_blocked = FALSE
    ),


namespace_most_matched_account AS (
    SELECT
      namespace_map.namespace_id,
      most_matched_namespace_account.account_id,
      MAX(IFF(salseforce_company_id.dim_crm_account_id IS NOT NULL, 1, 0)) AS mma_account_id_in_sfzid
    FROM namespace_map
-- based on the highest match count account
    INNER JOIN most_matched_namespace_account
      ON most_matched_namespace_account.namespace_id = namespace_map.namespace_id -- namespace level: most matched accounts by the users of the namespace
    LEFT JOIN salseforce_company_id
      ON salseforce_company_id.source_company_id = namespace_map.predicted_company_id AND salseforce_company_id.dim_crm_account_id = most_matched_namespace_account.account_id -- account level
    WHERE namespace_map.predicted_company_id IS NOT NULL
    GROUP BY 1, 2
  ),

namesapce_email_domain_account AS (
    SELECT
      namespace_map.namespace_id,
      account_email_domain.dim_crm_account_id AS account_id,
      account_email_domain.ratio_of_users,
      IFF(salseforce_company_id.source_company_id IS NOT NULL, 1, 0) AS edom_account_id_in_sfzid,
      ROW_NUMBER() OVER (PARTITION BY namespace_map.namespace_id ORDER BY edom_account_id_in_sfzid DESC , account_email_domain.ratio_of_users DESC ) AS rn
    FROM namespace_map
    INNER JOIN namespace_email_domain
      ON namespace_email_domain.namespace_id = namespace_map.namespace_id AND namespace_email_domain.row_number = 1
    INNER JOIN account_email_domain
      ON account_email_domain.rn = 1 AND account_email_domain.business_email = 1 AND account_email_domain.email_domain = namespace_email_domain.email_domain
    LEFT JOIN salseforce_company_id
      ON salseforce_company_id.source_company_id = namespace_map.predicted_company_id AND account_email_domain.dim_crm_account_id = salseforce_company_id.dim_crm_account_id
    WHERE namespace_map.predicted_company_id IS NOT NULL
  ),

namespace_map_details AS (
    SELECT
      namespace_map.*,
      COALESCE(IFF(company_ids.no_a = 1, company_ids.account_id, NULL), namespace_most_matched_account.account_id, namesapce_email_domain_account.account_id) AS zi_linked_account,
      COALESCE(namespace_map.account_id, zi_linked_account) AS combined_account_id,
      CASE
        WHEN namespace_map.actual_account_id IS NOT NULL THEN 'actual_account'
        WHEN namespace_map.account_id IS NOT NULL THEN 'Predicted_account'
        WHEN zi_linked_account IS NOT NULL THEN 'Zi_linked_account'
        WHEN company_ids.no_a IS NOT NULL THEN 'Zi_linked_Multi_account'
        WHEN namespace_map.predicted_company_id IS NOT NULL THEN 'Zi_linked_but_no_account_match'
        ELSE 'None'
      END AS match_account_type,
      company_ids.no_a,
      company_ids.no_upa,
      company_ids.list_of_accounts,
      company_ids.list_of_upa
    FROM namespace_map
    LEFT JOIN namespace_most_matched_account
      ON namespace_most_matched_account.namespace_id = namespace_map.namespace_id AND namespace_most_matched_account.mma_account_id_in_sfzid = 1
    LEFT JOIN namesapce_email_domain_account
      ON namesapce_email_domain_account.namespace_id = namespace_map.namespace_id AND namesapce_email_domain_account.rn = 1 AND namesapce_email_domain_account.edom_account_id_in_sfzid = 1
    LEFT JOIN company_ids
      ON company_ids.source_company_id = namespace_map.company_id -- identified company
)

SELECT * from namespace_map_details
