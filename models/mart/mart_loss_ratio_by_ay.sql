{{ config(materialized='table') }}

SELECT
    u.accident_year,
    e.earned_premium,
    u.ultimate_loss,
    u.ultimate_loss / NULLIF(e.earned_premium, 0) AS loss_ratio
FROM {{ ref('mart_ultimate_loss') }} u
LEFT JOIN {{ ref('int_exposure_summary') }} e
    ON u.accident_year = e.accident_year
ORDER BY 1
