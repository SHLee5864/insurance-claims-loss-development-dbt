with source as (
    select * from {{ source('raw', 'raw_reserve_snapshot') }}
),

renamed as (
    select
        claim_id,
        cast(valuation_date as date) as valuation_date,
        cast(case_reserve_amount as double) as case_reserve_amount,
        cast(expense_reserve_amount as double) as expense_reserve_amount
    from source
)

select * from renamed
