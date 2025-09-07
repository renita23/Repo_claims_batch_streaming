with claim_source as (
    select * from {{ ref('stg_allclaims') }}
)
select
    submission_date,
    count(*) as total_claims,
    sum(claim_amount) as total_claim_amount,
    sum(case when claim_severity = 'High' then 1 else 0 end) as high_severity_count,
    sum(case when claim_severity = 'Medium' then 1 else 0 end) as medium_severity_count,
    sum(case when claim_severity = 'Low' then 1 else 0 end) as low_severity_count
from claim_source
group by submission_date
{{ config(materialized='table') }}
