with source as (
    select * from {{ source('raw', 'raw_transactions') }}
),

renamed as (
    select
        transaction_id,
        claim_id,
        event_type,
        cast(event_timestamp as timestamp) as event_timestamp,
        handler_id,
        auto_or_manual,
        comment
    from source
)

select * from renamed
