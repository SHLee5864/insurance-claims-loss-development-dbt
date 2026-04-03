{% test accident_before_report_date(model, column_name) %}

select *
from {{ model }}
where accident_date > reported_date

{% endtest %}
