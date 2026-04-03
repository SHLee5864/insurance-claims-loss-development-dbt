with source as (
    select * from {{ source('raw', 'raw_exposure') }}
),

renamed as (
    select
        policy_number as policy_number,
        policy_holder_id,
        cast(inception_date as date) as inception_date,
        cast(expiration_date as date) as expiration_date,
        cast(date_month as date) as date_month,
        cast(premium_amount as double) as premium_amount,
        cast(earned_exposure as double) as earned_exposure,
        cast(earned_premium as double) as earned_premium
    from source
)

select * from renamed
