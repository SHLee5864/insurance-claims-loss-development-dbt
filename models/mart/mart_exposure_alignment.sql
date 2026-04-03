{{ config(materialized='table') }}

SELECT
    e.accident_year,
    e.region,
    e.earned_exposure,
    e.earned_premium,
    u.ultimate_loss,
    u.ultimate_loss / NULLIF(e.earned_premium, 0) AS loss_ratio
FROM {{ ref('int_exposure_summary') }} e
LEFT JOIN {{ ref('mart_ultimate_loss') }} u
    ON e.accident_year = u.accident_year
ORDER BY 1,2
