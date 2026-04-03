{{ config(
    materialized = 'view'
) }}

with reserve as (

    select
        r.claim_id,
        cast(r.valuation_date as date) as valuation_date,
        date_trunc('month', r.valuation_date) as valuation_month,
        r.case_reserve_amount,
        r.expense_reserve_amount
    from {{ ref('stg_reserve_snapshot') }} as r

),

claim_dim as (

    select
        claim_id,
        accident_year,
        accident_month
    from {{ ref('int_claim_base') }}

),

final as (

    select
        r.claim_id,
        cd.accident_year,
        cd.accident_month,
        r.valuation_date,
        r.valuation_month,
        r.case_reserve_amount,
        r.expense_reserve_amount
    from reserve r
    left join claim_dim cd
        on r.claim_id = cd.claim_id

)

select *
from final
