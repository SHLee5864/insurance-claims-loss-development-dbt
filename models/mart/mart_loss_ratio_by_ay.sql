{{ config(materialized='table') }}

WITH triangle AS (
    SELECT
        accident_year,
        valuation_year,
        dev_year,
        incurred_amount
    FROM {{ ref('mart_incurred_triangle') }}
),

ultimate_by_val AS (
    SELECT
        t.accident_year,
        t.valuation_year,
        t.dev_year,
        ROUND(t.incurred_amount, 2)                           AS latest_incurred,
        coalesce(ldf.cumulative_ldf, 1.0)                      AS cumulative_ldf,
        ROUND(t.incurred_amount * coalesce(ldf.cumulative_ldf, 1.0), 2) AS ultimate_loss
    FROM triangle t
    LEFT JOIN {{ ref('mart_ldf') }} ldf
        ON t.dev_year = ldf.dev_year_from
),

exposure AS (
    SELECT
        accident_year,
        SUM(earned_premium) AS earned_premium
    FROM {{ ref('int_exposure_summary') }}
    GROUP BY 1
)

SELECT
    u.accident_year,
    u.valuation_year,
    ROUND(u.latest_incurred, 2)                              AS latest_incurred,
    ROUND(u.cumulative_ldf, 4)                               AS cumulative_ldf,
    ROUND(u.ultimate_loss, 2)                                AS ultimate_loss,
    ROUND(e.earned_premium, 2)                               AS earned_premium,
    ROUND(u.ultimate_loss / NULLIF(e.earned_premium, 0), 4)  AS loss_ratio
FROM ultimate_by_val u
LEFT JOIN exposure e USING (accident_year)
ORDER BY u.accident_year, u.valuation_year