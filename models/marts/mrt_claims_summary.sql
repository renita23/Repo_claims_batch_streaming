with claim_source as (
    select * from {{ ref('stg_allclaims') }}
)

select 
    claim_type,
    claim_amount,
    source_type,
    count(*) as total_claims,
    sum(claim_amount) as total_amount
from claim_source
GROUP BY claim_type, claim_amount, source_type