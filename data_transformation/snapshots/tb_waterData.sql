{{
    config(
        materialized="table"
    )
}}

{{
    ingest_data('tb_waterData', 'tb_waterData')
}}