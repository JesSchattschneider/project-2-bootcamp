{% macro parse_api_response(source_name, table_name, time_column, value_columns, datatype, region) %}

WITH RAWDATA AS (
    SELECT
        *
    FROM
        {{ source(source_name, table_name) }}
    WHERE
        _AIRBYTE_EXTRACTED_AT = (
            SELECT
                MAX(_AIRBYTE_EXTRACTED_AT)
            FROM
                {{ source(source_name, table_name) }}
        )
),

exploded_time AS (
    SELECT
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        VALUE::TIMESTAMP_NTZ AS {{ time_column }},
        INDEX AS time_index
    FROM
        RAWDATA,
        LATERAL FLATTEN(input => TIME) -- Explode the TIME array
),

{% for column in value_columns %}
exploded_values_{{ loop.index }} AS (
    SELECT
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        VALUE::FLOAT AS {{ column }}, -- Cast VALUE to FLOAT if it's numerical, change as needed
        INDEX AS value_index
    FROM
        RAWDATA,
        LATERAL FLATTEN(input => "{{ column }}") -- Explode the VALUES array
){% if not loop.last %},{% endif %}
{% endfor %}

SELECT
    t._AIRBYTE_RAW_ID,
    t._AIRBYTE_EXTRACTED_AT,
    t._AIRBYTE_META,
    t.{{ time_column }} AS time,
    {% for column in value_columns %}
    v{{ loop.index }}.{{ column }} AS {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %},
    '{{ datatype }}' AS datatype,
    '{{ region }}' AS region
FROM
    exploded_time t
{% for column in value_columns %}
JOIN
    exploded_values_{{ loop.index }} v{{ loop.index }}
ON
    t._AIRBYTE_RAW_ID = v{{ loop.index }}._AIRBYTE_RAW_ID
    AND t._AIRBYTE_EXTRACTED_AT = v{{ loop.index }}._AIRBYTE_EXTRACTED_AT
    AND t._AIRBYTE_META = v{{ loop.index }}._AIRBYTE_META
    AND t.time_index = v{{ loop.index }}.value_index
{% endfor %}

{% endmacro %}
