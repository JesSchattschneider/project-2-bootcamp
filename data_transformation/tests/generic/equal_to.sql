{% test equal_to(model, column_name, value) %}
    select distinct {{ column_name }}
    from {{ model }}
    where {{ column_name }} = '{{ value }}'
{% endtest %}