{% macro calc_dev_month(accident_date, valuation_month) %}
    
    date_diff('month', {{ accident_date }}, {{ valuation_month }})
{% endmacro %}
