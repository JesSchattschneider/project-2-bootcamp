{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response('api_response', 'tb_windData', 'time', ['GUST', 'SPEED', 'DIRECTION'] , 'winddata', 'tb') }}
