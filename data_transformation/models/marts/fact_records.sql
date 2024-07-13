{{ config(
    materialized="incremental",
    incremental_strategy="delete+insert"
) }}

{% set max_time %}
  {% if is_incremental() %}
    (select max(time) from {{ this }})
  {% else %}
    null
  {% endif %}
{% endset %}

WITH temperature_data AS (
    SELECT
        TEMPERATURE_KEY,
        TIME,
        REGION,
        TEMPERATURE
    FROM {{ source('marts', 'DIM_TEMPERATURE') }}
    {% if is_incremental() %}
    WHERE TIME > {{ max_time }}
    {% endif %}
),

wind_data AS (
    SELECT
        WIND_KEY,
        TIME,
        REGION,
        GUST,
        SPEED,
        DIRECTION,
        CURRENT_TIMESTAMP() AS LAST_UPDATED
    FROM {{ source('marts', 'DIM_WIND') }}
    {% if is_incremental() %}
    WHERE TIME > {{ max_time }}
    {% endif %}
),

joined_data AS (
    SELECT
        COALESCE(t.TIME, w.TIME) AS TIME,
        COALESCE(t.REGION, w.REGION) AS REGION,
        t.TEMPERATURE_KEY,
        t.TEMPERATURE,
        w.WIND_KEY,
        w.GUST,
        w.SPEED,
        w.DIRECTION,
        COALESCE(w.LAST_UPDATED, CURRENT_TIMESTAMP()) AS LAST_UPDATED  -- Ensure LAST_UPDATED is always populated
    FROM temperature_data t
    FULL OUTER JOIN wind_data w
    ON t.TIME = w.TIME AND t.REGION = w.REGION
),

fact_data AS (
    SELECT
        md5(CONCAT(
            COALESCE(j.TEMPERATURE_KEY, ''),
            '-',
            COALESCE(j.WIND_KEY, '')
        )) AS fact_key,        
        j.TIME,
        j.REGION,
        j.TEMPERATURE_KEY AS temperature_key,  -- Directly use the TEMPERATURE_KEY
        j.WIND_KEY AS wind_key,  -- Directly use the WIND_KEY
        j.TEMPERATURE,
        j.GUST,
        j.SPEED,
        j.DIRECTION,
        j.LAST_UPDATED
    FROM joined_data j
)

SELECT * FROM fact_data
