{% test datetime_test(model, column_name) %}
select *
from {{ model }}
where not (typeof({{ column_name }}) = 'datetime')
{% endtest %}
