{% macro calc_dev_month(accident_date, valuation_month) %}
    -- DuckDB 기준 (Databricks는 macro override로 교체 가능)
    date_diff('month', {{ accident_date }}, {{ valuation_month }})
{% endmacro %}
