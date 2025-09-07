{{ config(materialized='view') }}

with batch_claims as (
    select
        claim_id,
        claim_type,
        claim_status,         
        trip_country,
        claim_amount,         
        SUBMISSION_DATE,           
        'BATCH' as source_type
    from {{ source('source1', 'batch_claims_staging') }}
    where claim_id is not null
),

streaming_claims as (
    select
        claim_id,
        claim_type,
        claim_status,
        trip_country,
        claim_amount,
        SUBMISSION_DATE,
        'STREAMING' as source_type
    from {{ source('source1', 'streaming_claims_staging') }}
    where claim_id is not null
)

select * from batch_claims
union all
select * from streaming_claims
