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
        CURRENT_TIMESTAMP() AS LAST_UPDATED  -- Define LAST_UPDATED here
    FROM {{ source('marts', 'DIM_WIND') }}
    {% if is_incremental() %}
    WHERE TIME > {{ max_time }}
    {% endif %}
),

joined_data AS (
    SELECT
        t.TIME AS temperature_time,
        t.REGION AS temperature_region,
        t.TEMPERATURE_KEY,
        t.TEMPERATURE,
        w.WIND_KEY,
        w.GUST,
        w.SPEED,
        w.DIRECTION,
        w.LAST_UPDATED  -- Include LAST_UPDATED from wind_data alias w
    FROM temperature_data t
    FULL OUTER JOIN wind_data w
    ON t.TIME = w.TIME AND t.REGION = w.REGION
),

fact_data AS (
    SELECT
        md5(CONCAT(
        COALESCE({{ dbt_utils.generate_surrogate_key(['j.TEMPERATURE_KEY']) }}, ''),
        '-',
        COALESCE({{ dbt_utils.generate_surrogate_key(['j.WIND_KEY']) }}, '')
    )) AS fact_key,        
        j.temperature_time AS TIME,
        j.temperature_region AS REGION,
        CASE WHEN j.TEMPERATURE_KEY IS NOT NULL THEN {{ dbt_utils.generate_surrogate_key(['j.TEMPERATURE_KEY']) }} END AS temperature_key,
        CASE WHEN j.WIND_KEY IS NOT NULL THEN {{ dbt_utils.generate_surrogate_key(['j.WIND_KEY']) }} END AS wind_key,
        j.TEMPERATURE,
        j.GUST,
        j.SPEED,
        j.DIRECTION,
        j.LAST_UPDATED  -- Refer to LAST_UPDATED from joined_data alias j
    FROM joined_data j
)

SELECT * FROM fact_data
