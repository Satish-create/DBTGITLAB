WITH libre AS (
    SELECT *
    FROM {{ ref('libre_hosts') }}
  /*
    This layers in the name of the company from Dorg, Cbit, and WHOIS.
  */

), dorg_joined AS (
  -- Get all dorg matches
    SELECT  dorg.company_name,
            libre.clean_domain AS the_clean_url,
            'DiscoverOrg'::varchar AS source
    FROM libre
    INNER JOIN {{ref('discoverorg_cache')}} AS dorg 
    ON libre.clean_domain = dorg.domain
    WHERE dorg.company_name IS NOT NULL

), dorg_remainder AS (
    -- This gets hosts records that did not match to dorg
    SELECT libre.clean_domain
    FROM libre
    LEFT OUTER JOIN dorg_joined 
    ON libre.clean_domain = dorg_joined.the_clean_url
    WHERE dorg_joined.the_clean_url IS NULL

), cbit_joined AS (
-- Get the clearbit matches from the remaining hosts after dorg matches
    SELECT  cbit.company_name,
            dorg_remainder.clean_domain AS the_clean_url,
            'Clearbit'::varchar AS source
    FROM dorg_remainder
    JOIN {{ref('clearbit_cache')}} AS cbit 
    ON dorg_remainder.clean_domain = cbit.domain
    WHERE cbit.company_name IS NOT NULL

), cbit_remainder AS (

    SELECT libre.clean_domain
    FROM libre
    LEFT OUTER JOIN dorg_joined 
    ON libre.clean_domain = dorg_joined.the_clean_url
    LEFT OUTER JOIN cbit_joined 
    ON libre.clean_domain = cbit_joined.the_clean_url
    WHERE dorg_joined.the_clean_url IS NULL AND
          cbit_joined.the_clean_url IS NULL

), whois_joined AS (

  SELECT
    whois.name                  AS company_name,
    cbit_remainder.clean_domain AS the_clean_url,
    'WHOIS'::varchar AS source
  FROM cbit_remainder
  JOIN {{ref('whois_cache')}} AS whois 
  ON cbit_remainder.clean_domain = whois.domain
  WHERE whois.name IS NOT NULL

)

SELECT * FROM dorg_joined

UNION ALL

SELECT * FROM cbit_joined

UNION ALL

SELECT * FROM whois_joined

ORDER BY the_clean_url