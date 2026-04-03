with source as (
    select * from {{ source('raw', 'raw_policy') }}
),

renamed as (
    select
        policy_number as policy_number,
        policy_holder_id,
        cast(inception_date as date) as inception_date,
        cast(expiration_date as date) as expiration_date,
        region as region,
        cast(premium_amount as double) as premium_amount,
        {{ dbt_utils.generate_surrogate_key(['policy_number']) }} as policy_key
    from source
)

select * from renamed
