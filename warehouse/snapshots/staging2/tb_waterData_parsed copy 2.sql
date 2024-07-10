{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response('tb_waterData', 'tb_waterData', 'time', 'value' , 'waterdata', 'tb') }}
