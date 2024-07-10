{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response('api_response', 'tb_wavesData', 'time', ['SIGNIFICANTHEIGHT', 'MAXHEIGHT', 'DIRECTION'] , 'wavesdata', 'tb') }}
