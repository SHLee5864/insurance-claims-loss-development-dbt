{{ config(materialized='table') }}

WITH tri AS (
    SELECT *
    FROM {{ ref('mart_incurred_triangle') }}
),

pairs AS (
    SELECT
        a.accident_year,
        a.dev_month AS dev_month_from,
        b.dev_month AS dev_month_to,
        a.incurred_amount AS incurred_from,
        b.incurred_amount AS incurred_to
    FROM tri a
    JOIN tri b
        ON a.accident_year = b.accident_year
       AND b.dev_month > a.dev_month
),

ldf_calc AS (
    SELECT
        dev_month_from,
        dev_month_to,
        SUM(incurred_to) / NULLIF(SUM(incurred_from), 0) AS ldf
    FROM pairs
    GROUP BY 1,2
),

cumulative AS (
    SELECT
        dev_month_from,
        dev_month_to,
        ldf,
        EXP(SUM(LOG(ldf)) OVER (ORDER BY dev_month_to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS cumulative_ldf
    FROM ldf_calc
)

SELECT
    dev_month_from,
    dev_month_to,
    ldf,
    cumulative_ldf
FROM cumulative
ORDER BY 1,2
