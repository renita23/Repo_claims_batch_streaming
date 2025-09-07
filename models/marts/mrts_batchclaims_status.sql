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


select
    upper(claim_status) as claim_status,
    count(*) as total
from claim_source
where claim_id is not null
group by upper(claim_status)
