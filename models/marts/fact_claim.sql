select
    claim_id,
    claim_amount,
    submission_date,
    claim_type,
    claim_status,
    trip_country,
    source_type
from {{ ref('stg_allclaims') }}
