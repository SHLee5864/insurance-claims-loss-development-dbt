{{ config(materialized='table') }}

WITH paid AS (
    SELECT
        claim_id,
        accident_year,
        accident_month,
        valuation_month,
        valuation_year,
        development_month,
        paid_to_date
    FROM {{ ref('int_dev_month') }}
    WHERE MONTH(valuation_month) = 12   -- ← 연말만
),

reserve AS (
    SELECT
        claim_id,
        valuation_month,
        case_reserve_amount + expense_reserve_amount AS reserve_amount
    FROM {{ ref('int_reserve_snapshot_enriched') }}
    WHERE MONTH(valuation_month) = 12   -- ← 연말만
),

joined AS (
    SELECT
        p.claim_id,
        p.accident_year,
        p.valuation_year,
        p.development_month          AS dev_month,
        p.paid_to_date,
        COALESCE(r.reserve_amount, 0)                   AS reserve_amount,
        p.paid_to_date + COALESCE(r.reserve_amount, 0) AS incurred_amount
    FROM paid p
    LEFT JOIN reserve r
        ON  p.claim_id       = r.claim_id
        AND p.valuation_month = r.valuation_month
)

SELECT
    accident_year,
    valuation_year,
    valuation_year - accident_year + 1 AS dev_year,
    ROUND(SUM(paid_to_date), 2)    AS paid_to_date,
    ROUND(SUM(reserve_amount), 2)  AS reserve_amount,
    ROUND(SUM(incurred_amount), 2) AS incurred_amount
FROM joined
GROUP BY 1, 2
ORDER BY 1, 2