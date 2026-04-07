{{ config(materialized='table') }}

WITH tri AS (
    SELECT
        accident_year,
        dev_year,
        incurred_amount
    FROM {{ ref('mart_incurred_triangle') }}
),

pairs AS (
    SELECT
        a.accident_year,
        a.dev_year       AS dev_year_from,
        b.dev_year       AS dev_year_to,
        a.incurred_amount AS incurred_from,
        b.incurred_amount AS incurred_to
    FROM tri a
    JOIN tri b
        ON a.accident_year = b.accident_year
        AND b.dev_year = a.dev_year + 1
    WHERE a.incurred_amount > 0
      AND b.incurred_amount > 0
),

avg_ldf AS (
    SELECT
        dev_year_from,
        dev_year_to,
        AVG(incurred_to / incurred_from) AS ldf
    FROM pairs
    GROUP BY dev_year_from, dev_year_to
),

cumulative AS (
    SELECT
        dev_year_from,
        dev_year_to,
        ldf,
        -- dev_year_from부터 끝까지의 누적 LDF
        -- 역순으로 곱해야 함
        EXP(SUM(LN(ldf)) OVER (
            ORDER BY dev_year_from DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )) AS cumulative_ldf
    FROM avg_ldf
)

SELECT * FROM cumulative
ORDER BY dev_year_from