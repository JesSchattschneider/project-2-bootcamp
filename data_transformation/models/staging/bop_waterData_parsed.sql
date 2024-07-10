{{
    config(
        materialized="table"
    )
}}


{{ parse_api_response_waterdata('api_response', 'bop_waterData', 'time', 'value' , 'waterdata', 'bop') }}
