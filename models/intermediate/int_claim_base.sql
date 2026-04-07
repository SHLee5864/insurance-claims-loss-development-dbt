{{ config(
    materialized = 'view'
) }}

with claims as (

    select
        c.claim_id,
        c.policy_number,
        c.accident_date,
        c.reported_date,
        extract(year from c.accident_date)   as accident_year,
        date_trunc('month', c.accident_date) as accident_month,
        c.loss_type,
        c.claimant_age,
        c.claim_key
    from {{ ref('stg_claims') }} as c

),

policy_enriched as (

    select
        cl.*,
        p.region as policy_region
    from claims cl
    left join {{ ref('stg_policy') }} as p
        on cl.policy_number = p.policy_number

),

calendar_flags as (
    select
        date_month,
        is_covid_wave,
        is_covid_lockdown,
        is_weather_event,
        travel_boom
    from {{ ref('stg_calendar') }}
),

final as (

    select
        p.claim_id,
        p.claim_key,
        p.policy_number,
        p.accident_date,
        p.reported_date,
        p.accident_year,
        p.accident_month,
        p.loss_type,
        p.claimant_age,

        -- Region sourced from policy data
        p.policy_region as region,

        -- Macro flags based on accident month
        cf.is_covid_wave       as covid_wave_flag,
        cf.is_covid_lockdown   as covid_lockdown_flag,
        cf.is_weather_event    as weather_event_flag,
        cf.travel_boom         as travel_boom_flag

    from policy_enriched p
    left join calendar_flags cf
        on p.accident_month = cf.date_month

)

select *
from final
