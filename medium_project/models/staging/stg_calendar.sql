with source as (
    select * from {{ source('raw', 'calendar') }}
),

renamed as (
    select
        cast(date_month as date) as date_month,
        cast(is_covid_wave as boolean) as is_covid_wave,
        cast(is_covid_lockdown as boolean) as is_covid_lockdown,
        cast(is_weather_event as boolean) as is_weather_event,
        cast(travel_boom as boolean) as travel_boom,
        cast(inflation_factor as double) as macro_inflation_factor
    from source
)

select * from renamed
