-- models/marts/mrt_claims_trend.sql
with claim_source as (
    select * from {{ ref('fact_claim') }}
)
select
    date_trunc('day', SUBMISSION_DATE) as day,
    claim_type,
    count(*) as total_claims,
    sum(claim_amount) as total_amount
from claim_source
group by date_trunc('day', SUBMISSION_DATE), claim_type
order by day
