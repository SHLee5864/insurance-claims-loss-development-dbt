{{ config(materialized='table') }}

WITH latest_val AS (
    SELECT MAX(valuation_year) AS max_val_year
    FROM {{ ref('mart_incurred_triangle') }}
),

latest_incurred AS (
    SELECT
        t.accident_year,
        t.valuation_year,
        t.dev_year,
        t.incurred_amount
    FROM {{ ref('mart_incurred_triangle') }} t
    JOIN latest_val ON t.valuation_year = latest_val.max_val_year
),

ldf_latest AS (
    SELECT dev_year_from, cumulative_ldf
    FROM {{ ref('mart_ldf') }}
),

exposure AS (
    SELECT accident_year, SUM(earned_premium) AS earned_premium
    FROM {{ ref('int_exposure_summary') }}
    GROUP BY 1
)

SELECT
    li.accident_year,
    li.valuation_year,
    li.dev_year,
    ROUND(li.incurred_amount, 2)                          AS latest_incurred,
    ROUND(coalesce(ldf.cumulative_ldf, 1.0), 4)           AS cumulative_ldf,
    ROUND(li.incurred_amount * coalesce(ldf.cumulative_ldf, 1.0), 2)     AS ultimate_loss,
    ROUND(e.earned_premium, 2)                             AS earned_premium,
    ROUND(li.incurred_amount * coalesce(ldf.cumulative_ldf, 1.0)
          / NULLIF(e.earned_premium, 0), 4)               AS loss_ratio
FROM latest_incurred li
LEFT JOIN ldf_latest ldf
    ON li.dev_year = ldf.dev_year_from
LEFT JOIN exposure e USING (accident_year)
ORDER BY li.accident_year