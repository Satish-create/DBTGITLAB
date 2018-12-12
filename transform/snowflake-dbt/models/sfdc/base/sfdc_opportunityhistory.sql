WITH base AS (
  
    SELECT *
    FROM raw.salesforce_stitch.opportunityhistory
)

SELECT  *,
      datediff(day, createddate, 
        lead(createddate) OVER (PARTITION BY OPPORTUNITYID ORDER BY CREATEDDATE)) AS days_in_stage
FROM base