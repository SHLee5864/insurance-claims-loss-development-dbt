{{ config(
    materialized = 'view'
) }}

with exposure as (

    select
        policy_number,
        region,
        date_month,
        earned_exposure,
        earned_premium
    from {{ ref('stg_exposure') }}

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
