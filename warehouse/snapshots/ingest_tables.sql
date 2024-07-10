{% macro ingest_data(table_name, dest_table_name) %}

-- select one row where the _AIRBYTE_EXTRACTED_AT was the most recent
select
    *
from {{ source(table_name, dest_table_name) }}
where _AIRBYTE_EXTRACTED_AT = (
    select
        max(_AIRBYTE_EXTRACTED_AT)
    from {{ source(table_name, dest_table_name) }}
)

{% endmacro %}
