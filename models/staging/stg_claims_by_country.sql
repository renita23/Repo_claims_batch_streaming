with claim_source as (
    select * 
    from {{ ref('stg_allclaims') }}
)

select
    sum(case when upper(claim_type) = 'MEDICAL' then 1 else 0 end) as medical_claims,
    sum(case when upper(claim_type) = 'NON-MEDICAL' then 1 else 0 end) as non_medical_claims,
    sum(case when upper(claim_type) = 'DENTAL' then 1 else 0 end) as dental_claims
from claim_source
where claim_id is not null

)

WITH country_counts AS (
    SELECT UPPER(TRIP_COUNTRY) AS COUNTRY, COUNT(*) AS claim_count
    FROM RAW.BATCH_CLAIMS_STAGING
    WHERE CLAIM_ID IS NOT NULL
    GROUP BY UPPER(TRIP_COUNTRY)
)
SELECT COUNTRY, claim_count
FROM country_counts
WHERE claim_count = (SELECT MAX(claim_count) FROM country_counts);
