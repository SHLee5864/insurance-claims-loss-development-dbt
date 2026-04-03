{{ config(
    materialized = 'view'
) }}

with claim_dim as (

    select
        claim_id,
        accident_month,
        accident_year
    from {{ ref('int_claim_base') }}

),

calendar as (

    select
        date_month
    from {{ ref('stg_calendar') }}
),

-- Claim × Month date spine 생성
claim_months as (

    select
        cd.claim_id,
        cd.accident_year,
        cd.accident_month,
        cal.date_month as valuation_month
    from claim_dim cd
    join calendar cal
        on cal.date_month >= cd.accident_month
),

payments as (

    select
        claim_id,
        payment_month,
        payment_amount_inflated
    from {{ ref('int_payments_enriched') }}

),

-- 월별 지급액 집계
paid_monthly as (

    select
        cm.claim_id,
        cm.accident_year,
        cm.accident_month,
        cm.valuation_month,
        coalesce(sum(p.payment_amount_inflated), 0) as paid_in_month
    from claim_months cm
    left join payments p
        on cm.claim_id = p.claim_id
       and cm.valuation_month = p.payment_month
    group by
        cm.claim_id,
        cm.accident_year,
        cm.accident_month,
        cm.valuation_month
),

-- 누적 지급액 계산
final as (

    select
        claim_id,
        accident_year,
        accident_month,
        valuation_month,
        paid_in_month,
        sum(paid_in_month) over (
            partition by claim_id
            order by valuation_month
            rows between unbounded preceding and current row
        ) as paid_to_date
    from paid_monthly
)

select *
from final
