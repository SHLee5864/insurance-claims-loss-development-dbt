{{ config(
    materialized = 'view'
) }}

with exposure as (

    select
        e.policy_number,
        e.date_month,
        e.earned_exposure,
        e.earned_premium,
        p.region
    from {{ ref('stg_exposure') }} e
    left join {{ ref('stg_policy') }} p using (policy_number)

),

with_ay as (

    select
        region,
        extract(year from date_month) as accident_year,
        earned_exposure,
        earned_premium
    from exposure
),

final as (

    select
        accident_year,
        region,
        sum(earned_exposure) as earned_exposure,
        sum(earned_premium)  as earned_premium
    from with_ay
    group by
        accident_year,
        region
)

select *
from final
