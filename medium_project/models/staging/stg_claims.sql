with source as (
    select * from {{ source('raw', 'raw_claims') }}
),

renamed as (
    select
        claim_id as claim_id,
        policy_number as policy_number,
        cast(accident_date as date) as accident_date,
        cast(reported_date as date) as reported_date,
        loss_type as loss_type,
        coalesce(region, 'unknown') as region,
        cast(claimant_age as int) as claimant_age,
        {{ dbt_utils.generate_surrogate_key(['claim_id']) }} as claim_key
    from source
)

select * from renamed
