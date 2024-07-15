{% macro parse_api_response_winddata(source_name, table_name, time_column, gust_column, direction_column, speed_column, datatype, region) %}

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

exploded_gust AS (
    SELECT
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        VALUE::FLOAT AS {{ gust_column }}, -- Cast VALUE to FLOAT for gust
        INDEX AS gust_index
    FROM
        RAWDATA,
        LATERAL FLATTEN(input => "GUST") -- Explode the GUST array
),

exploded_direction AS (
    SELECT
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        VALUE::FLOAT AS {{ direction_column }}, -- Cast VALUE to FLOAT for direction
        INDEX AS direction_index
    FROM
        RAWDATA,
        LATERAL FLATTEN(input => "DIRECTION") -- Explode the DIRECTION array
),

exploded_speed AS (
    SELECT
        _AIRBYTE_RAW_ID,
        _AIRBYTE_EXTRACTED_AT,
        _AIRBYTE_META,
        VALUE::FLOAT AS {{ speed_column }}, -- Cast VALUE to FLOAT for speed
        INDEX AS speed_index
    FROM
        RAWDATA,
        LATERAL FLATTEN(input => "SPEED") -- Explode the SPEED array
)

SELECT
    t._AIRBYTE_RAW_ID,
    t._AIRBYTE_EXTRACTED_AT,
    t._AIRBYTE_META,
    t.{{ time_column }} AS time,
    g.{{ gust_column }} AS gust,
    d.{{ direction_column }} AS direction,
    s.{{ speed_column }} AS speed,
    '{{ datatype }}' AS datatype,
    '{{ region }}' AS region
FROM
    exploded_time t
JOIN
    exploded_gust g
    ON  t._AIRBYTE_RAW_ID = g._AIRBYTE_RAW_ID
    AND t._AIRBYTE_EXTRACTED_AT = g._AIRBYTE_EXTRACTED_AT
    AND t._AIRBYTE_META = g._AIRBYTE_META
    AND t.time_index = g.gust_index
JOIN
    exploded_direction d
    ON  t._AIRBYTE_RAW_ID = d._AIRBYTE_RAW_ID
    AND t._AIRBYTE_EXTRACTED_AT = d._AIRBYTE_EXTRACTED_AT
    AND t._AIRBYTE_META = d._AIRBYTE_META
    AND t.time_index = d.direction_index
JOIN
    exploded_speed s
    ON  t._AIRBYTE_RAW_ID = s._AIRBYTE_RAW_ID
    AND t._AIRBYTE_EXTRACTED_AT = s._AIRBYTE_EXTRACTED_AT
    AND t._AIRBYTE_META = s._AIRBYTE_META
    AND t.time_index = s.speed_index

{% endmacro %}
