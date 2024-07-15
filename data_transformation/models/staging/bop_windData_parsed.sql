{% set source_name = 'api_response' %}
{% set table_name = 'bop_windData' %}

{% if execute %}

-- Check if the time column exists in the source table
{% set column_check_query %}
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{{ table_name }}'
AND COLUMN_NAME = 'time'
{% endset %}

{% set column_exists = run_query(column_check_query) %}

{% if column_exists.rows | length > 0 %}

-- Execute the macro to parse wind data
{{ parse_api_response_winddata(source_name, table_name, 'time', 'GUST', 'DIRECTION', 'SPEED', 'winddata', 'bop') }}

{% else %}

-- Log a message indicating the time column does not exist
SELECT 'Skipping macro execution because time column does not exist in source table' AS skip_reason

{% endif %}

{% else %}

-- Skip execution
SELECT 'Execution disabled' AS skip_reason

{% endif %}