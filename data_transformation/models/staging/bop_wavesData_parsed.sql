{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response('api_response', 'bop_wavesData', 'time', ['SIGNIFICANTHEIGHT', 'MAXHEIGHT', 'DIRECTION'] , 'wavesdata', 'bop') }}
