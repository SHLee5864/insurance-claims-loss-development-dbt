{{ config(materialized='table') }}

WITH paid AS (
    SELECT
        claim_id,
        accident_year,
        development_month,
        paid_to_date
    FROM {{ ref('int_dev_month') }}
),

reserve AS (
    SELECT
        claim_id,
        accident_year,
        valuation_month,
        case_reserve_amount + expense_reserve_amount AS reserve_amount
    FROM {{ ref('int_reserve_snapshot_enriched') }}
),

joined AS (
    SELECT
        p.claim_id,
        p.accident_year,
        p.development_month AS dev_month,
        p.paid_to_date,
        COALESCE(r.reserve_amount, 0) AS reserve_amount,
        p.paid_to_date + COALESCE(r.reserve_amount, 0) AS incurred_amount
    FROM paid p
    LEFT JOIN reserve r
        ON p.claim_id = r.claim_id
       AND p.development_month = datediff('month', make_date(p.accident_year, 1, 1), r.valuation_month)
)

SELECT
    accident_year,
    dev_month,
    SUM(paid_to_date) AS paid_to_date,
    SUM(reserve_amount) AS reserve_amount,
    SUM(incurred_amount) AS incurred_amount
FROM joined
GROUP BY 1,2
ORDER BY 1,2
