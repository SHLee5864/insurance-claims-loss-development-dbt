{{ config(materialized='table') }}

WITH latest_dm AS (
    SELECT
        accident_year,
        MAX(dev_month) AS latest_dev_month
    FROM {{ ref('mart_incurred_triangle') }}
    GROUP BY 1
),

latest_incurred AS (
    SELECT
        t.accident_year,
        t.dev_month,
        t.incurred_amount
    FROM {{ ref('mart_incurred_triangle') }} t
),

ldf AS (
    SELECT
        dev_month_from,
        MAX(cumulative_ldf) AS cumulative_ldf
    FROM {{ ref('mart_ldf') }}
    GROUP BY 1
)

SELECT
    l.accident_year,
    l.latest_dev_month,
    i.incurred_amount AS latest_incurred,
    d.cumulative_ldf,
    i.incurred_amount * d.cumulative_ldf AS ultimate_loss
FROM latest_dm l
JOIN latest_incurred i
    ON l.accident_year = i.accident_year
   AND l.latest_dev_month = i.dev_month
LEFT JOIN ldf d
    ON d.dev_month_from = l.latest_dev_month
