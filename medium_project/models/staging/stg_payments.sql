with source as (
    select * from {{ source('raw', 'raw_payments') }}
),

renamed as (
    select
        payment_id as payment_id,
        claim_id as claim_id,
        cast(payment_date as date) as payment_date,
        cast(payment_amount as double) as payment_amount
    from source
)

select * from renamed
