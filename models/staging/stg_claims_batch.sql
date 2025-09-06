with claim_source as (
    select * 
    from {{ source("source1",batch_claims_staging)}}
)
    
SELECT
    SUM(CASE WHEN UPPER(claim_type) = 'MEDICAL' THEN 1 ELSE 0 END) AS medical_claims,
    SUM(CASE WHEN UPPER(claim_type) = 'NON-MEDICAL' THEN 1 ELSE 0 END) AS non_medical_claims,
    SUM(CASE WHEN UPPER(claim_type) = 'DENTAL' THEN 1 ELSE 0 END) AS dental_claims
FROM RAW.BATCH_CLAIMS_STAGING
WHERE claim_id IS NOT NULL;

SELECT UPPER(CLAIM_STATUS) AS CLAIM_STATUS, COUNT(*) AS TOTAL
FROM claim_source WHERE claim_id IS NOT NULL
GROUP BY CLAIM_STATUS