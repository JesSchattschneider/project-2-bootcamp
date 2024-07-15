{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response_winddata('api_response', 'tb_windData', 'time', 'GUST', 'DIRECTION', 'SPEED', 'winddata', 'tb') }}
