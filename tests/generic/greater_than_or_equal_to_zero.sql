{% test greater_than_or_equal_to_zero(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
