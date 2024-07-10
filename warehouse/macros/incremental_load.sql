{% macro incremental_load_with_hist(source_hist_name, hist_table, staging_parsed_model, incremental_column, columns) %}

{{ config(
    materialized="incremental",
    incremental_strategy="delete+insert"
) }}

{% if not is_incremental() %}
    -- Full refresh: Create the table from scratch
    WITH hist AS (
        SELECT {{ columns }} FROM {{ source(source_hist_name, hist_table) }}
    ),
    parsed AS (
        SELECT {{ columns }} FROM {{ ref(staging_parsed_model) }}
    )
    SELECT * FROM hist
    UNION ALL
    SELECT * FROM parsed
{% else %}
    -- Incremental load: Append new records from parsed_table
    WITH max_time AS (
        SELECT MAX({{ incremental_column }}) AS max_time FROM {{ this }}
    ),
    new_data AS (
        SELECT {{ columns }}
        FROM {{ ref(staging_parsed_model) }}
        WHERE {{ incremental_column }} > (SELECT max_time FROM max_time)
    )
    SELECT * FROM new_data
{% endif %}

{% endmacro %}
