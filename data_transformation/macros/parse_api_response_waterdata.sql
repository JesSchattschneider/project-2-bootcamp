{% macro parse_api_response_waterdata(source_name, table_name, time_column, value_column, datatype, region) %}

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

exploded_values AS (
    SELECT
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        VALUE::FLOAT AS {{ value_column }}, -- Cast VALUE to FLOAT if it's numerical, change as needed
        INDEX AS value_index
    FROM
        RAWDATA,
        LATERAL FLATTEN(input => "VALUES") -- Explode the VALUES array
)

SELECT
    t._AIRBYTE_RAW_ID,
    t._AIRBYTE_EXTRACTED_AT,
    t._AIRBYTE_META,
    t.{{ time_column }} AS time,
    v.{{ value_column }} AS value,
    '{{ datatype }}' AS datatype,
    '{{ region }}' AS region
FROM
    exploded_time t
JOIN
    exploded_values v
ON
    t._AIRBYTE_RAW_ID = v._AIRBYTE_RAW_ID
    AND t._AIRBYTE_EXTRACTED_AT = v._AIRBYTE_EXTRACTED_AT
    AND t._AIRBYTE_META = v._AIRBYTE_META
    AND t.time_index = v.value_index

{% endmacro %}