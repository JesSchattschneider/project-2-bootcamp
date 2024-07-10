{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response_waterdata('api_response', 'tb_waterData', 'time', 'value' , 'waterdata', 'tb') }}
