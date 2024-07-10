{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response('api_response', 'bop_windData', 'time', ['GUST', 'SPEED', 'DIRECTION'] , 'winddata', 'bop') }}
