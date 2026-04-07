{{ config(
    materialized = 'view'
) }}

with payments as (

    select
        p.payment_id,
        p.claim_id,
        p.payment_date,
        date_trunc('month', p.payment_date) as payment_month,
        extract(year from p.payment_date)   as payment_year,
        p.payment_amount
    from {{ ref('stg_payments') }} as p

),

claim_dim as (

    select
        claim_id,
        accident_year
    from {{ ref('int_claim_base') }}

),

calendar_infl as (

    select
        date_month,
        macro_inflation_factor
    from {{ ref('stg_calendar') }}

),

final as (

    select
        pay.payment_id,
        pay.claim_id,
        pay.payment_date,
        pay.payment_month,
        pay.payment_year,
        pay.payment_amount,

        -- inflation factor (payment_month 기준)
        cal.macro_inflation_factor as inflation_factor,

        -- inflation 적용 지급액
        round(pay.payment_amount * cal.macro_inflation_factor, 2) as payment_amount_inflated,

        -- accident_year 상속
        cd.accident_year

    from payments pay
    left join claim_dim cd
        on pay.claim_id = cd.claim_id
    left join calendar_infl cal
        on pay.payment_month = cal.date_month

)

select *
from final
