{{ config(
    materialized='table'
) }}

WITH aggregated_data AS (
    SELECT
        'TEMPERATURE' AS variable_name,
        REGION,
        DATE_TRUNC('month', TIME) AS month,
        AVG(TEMPERATURE) AS mean_value,
        MAX(TEMPERATURE) AS max_value,
        MIN(TEMPERATURE) AS min_value,
        COUNT(TEMPERATURE) AS count,
        MIN(TIME) AS date_of_first_record,
        MAX(TIME) AS date_of_last_record,
        MAX(LAST_UPDATED) AS last_updated
    FROM {{ ref('fact_records') }}
    WHERE TEMPERATURE IS NOT NULL
    GROUP BY REGION, DATE_TRUNC('month', TIME)

    UNION ALL

    SELECT
        'GUST' AS variable_name,
        REGION,
        DATE_TRUNC('month', TIME) AS month,
        AVG(GUST) AS mean_value,
        MAX(GUST) AS max_value,
        MIN(GUST) AS min_value,
        COUNT(GUST) AS count,
        MIN(TIME) AS date_of_first_record,
        MAX(TIME) AS date_of_last_record,
        MAX(LAST_UPDATED) AS last_updated
    FROM {{ ref('fact_records') }}
    WHERE GUST IS NOT NULL
    GROUP BY REGION, DATE_TRUNC('month', TIME)

    UNION ALL

    SELECT
        'SPEED' AS variable_name,
        REGION,
        DATE_TRUNC('month', TIME) AS month,
        AVG(SPEED) AS mean_value,
        MAX(SPEED) AS max_value,
        MIN(SPEED) AS min_value,
        COUNT(SPEED) AS count,
        MIN(TIME) AS date_of_first_record,
        MAX(TIME) AS date_of_last_record,
        MAX(LAST_UPDATED) AS last_updated
    FROM {{ ref('fact_records') }}
    WHERE SPEED IS NOT NULL
    GROUP BY REGION, DATE_TRUNC('month', TIME)

    UNION ALL

    SELECT
        'DIRECTION' AS variable_name,
        REGION,
        DATE_TRUNC('month', TIME) AS month,
        AVG(DIRECTION) AS mean_value,
        MAX(DIRECTION) AS max_value,
        MIN(DIRECTION) AS min_value,
        COUNT(DIRECTION) AS count,
        MIN(TIME) AS date_of_first_record,
        MAX(TIME) AS date_of_last_record,
        MAX(LAST_UPDATED) AS last_updated
    FROM {{ ref('fact_records') }}
    WHERE DIRECTION IS NOT NULL
    GROUP BY REGION, DATE_TRUNC('month', TIME)
)

SELECT * FROM aggregated_data
ORDER BY REGION, month, variable_name
