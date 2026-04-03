{{ config(
    materialized = 'view'
) }}

with base as (

    select
        claim_id,
        accident_year,
        accident_month,
        valuation_month,
        paid_in_month,
        paid_to_date
    from {{ ref('int_claim_monthly_paid') }}

),

final as (

    select
        claim_id,
        accident_year,
        accident_month,
        valuation_month,

        -- development month 계산
        datediff('month', accident_month, valuation_month) as development_month,

        -- optional: development year/quarter
        extract(year from valuation_month) - extract(year from accident_month) as development_year,
        (datediff('month', accident_month, valuation_month)) / 3 as development_quarter,

        paid_in_month,
        paid_to_date

    from base
)

select *
from final
