{{ config(
    materialized='table'
) }}

WITH temperature_data AS (
    SELECT
        REGION,
        TIME,
        TEMPERATURE,
        ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY TIME DESC) AS rn
    FROM {{ ref('fact_records') }}
    WHERE TEMPERATURE IS NOT NULL
),
last_temperature AS (
    SELECT
        REGION,
        TEMPERATURE AS last_temperature
    FROM temperature_data
    WHERE rn = 1
),
gust_data AS (
    SELECT
        REGION,
        TIME,
        GUST,
        ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY TIME DESC) AS rn
    FROM {{ ref('fact_records') }}
    WHERE GUST IS NOT NULL
),
last_gust AS (
    SELECT
        REGION,
        GUST AS last_gust
    FROM gust_data
    WHERE rn = 1
),
speed_data AS (
    SELECT
        REGION,
        TIME,
        SPEED,
        ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY TIME DESC) AS rn
    FROM {{ ref('fact_records') }}
    WHERE SPEED IS NOT NULL
),
last_speed AS (
    SELECT
        REGION,
        SPEED AS last_speed
    FROM speed_data
    WHERE rn = 1
),
direction_data AS (
    SELECT
        REGION,
        TIME,
        DIRECTION,
        ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY TIME DESC) AS rn
    FROM {{ ref('fact_records') }}
    WHERE DIRECTION IS NOT NULL
),
last_direction AS (
    SELECT
        REGION,
        DIRECTION AS last_direction
    FROM direction_data
    WHERE rn = 1
),
aggregated_data AS (
    SELECT
        'TEMPERATURE' AS variable_name,
        fr.REGION,
        AVG(fr.TEMPERATURE) AS mean_value,
        MAX(fr.TEMPERATURE) AS max_value,
        MIN(fr.TEMPERATURE) AS min_value,
        COUNT(fr.TEMPERATURE) AS count,
        MIN(fr.TIME) AS date_of_first_record,
        MAX(fr.TIME) AS date_of_last_record,
        MAX(fr.LAST_UPDATED) AS last_updated,
        lt.last_temperature AS last_value
    FROM {{ ref('fact_records') }} fr
    LEFT JOIN last_temperature lt ON fr.REGION = lt.REGION
    WHERE fr.TEMPERATURE IS NOT NULL
    GROUP BY fr.REGION, lt.last_temperature

    UNION ALL

    SELECT
        'GUST' AS variable_name,
        fr.REGION,
        AVG(fr.GUST) AS mean_value,
        MAX(fr.GUST) AS max_value,
        MIN(fr.GUST) AS min_value,
        COUNT(fr.GUST) AS count,
        MIN(fr.TIME) AS date_of_first_record,
        MAX(fr.TIME) AS date_of_last_record,
        MAX(fr.LAST_UPDATED) AS last_updated,
        lg.last_gust AS last_value
    FROM {{ ref('fact_records') }} fr
    LEFT JOIN last_gust lg ON fr.REGION = lg.REGION
    WHERE fr.GUST IS NOT NULL
    GROUP BY fr.REGION, lg.last_gust

    UNION ALL

    SELECT
        'SPEED' AS variable_name,
        fr.REGION,
        AVG(fr.SPEED) AS mean_value,
        MAX(fr.SPEED) AS max_value,
        MIN(fr.SPEED) AS min_value,
        COUNT(fr.SPEED) AS count,
        MIN(fr.TIME) AS date_of_first_record,
        MAX(fr.TIME) AS date_of_last_record,
        MAX(fr.LAST_UPDATED) AS last_updated,
        ls.last_speed AS last_value
    FROM {{ ref('fact_records') }} fr
    LEFT JOIN last_speed ls ON fr.REGION = ls.REGION
    WHERE fr.SPEED IS NOT NULL
    GROUP BY fr.REGION, ls.last_speed

    UNION ALL

    SELECT
        'DIRECTION' AS variable_name,
        fr.REGION,
        AVG(fr.DIRECTION) AS mean_value,
        MAX(fr.DIRECTION) AS max_value,
        MIN(fr.DIRECTION) AS min_value,
        COUNT(fr.DIRECTION) AS count,
        MIN(fr.TIME) AS date_of_first_record,
        MAX(fr.TIME) AS date_of_last_record,
        MAX(fr.LAST_UPDATED) AS last_updated,
        ld.last_direction AS last_value
    FROM {{ ref('fact_records') }} fr
    LEFT JOIN last_direction ld ON fr.REGION = ld.REGION
    WHERE fr.DIRECTION IS NOT NULL
    GROUP BY fr.REGION, ld.last_direction
)

SELECT * FROM aggregated_data
