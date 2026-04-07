{{ config(materialized='table') }}

WITH base AS (
    SELECT
        dm.claim_id,
        cb.accident_year,
        dm.development_month AS dev_month,
        dm.paid_to_date,
        cb.region,
        cb.loss_type,
        cb.claimant_age
    FROM {{ ref('int_dev_month') }} dm
    LEFT JOIN {{ ref('int_claim_base') }} cb
        ON dm.claim_id = cb.claim_id
)

SELECT
    accident_year,
    dev_month,
    SUM(paid_to_date) AS paid_to_date,
    COUNT(DISTINCT claim_id) AS n_claims
FROM base
GROUP BY 1,2
ORDER BY 1,2
