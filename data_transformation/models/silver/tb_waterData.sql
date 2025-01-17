-- depends_on: {{ ref('tb_waterData_parsed') }}

{{
    incremental_load_with_hist(
        source_hist_name='historical_data', 
        hist_table='tb_waterData_hist', 
        staging_parsed_model='tb_waterData_parsed', 
        incremental_column='TIME',
        columns='TIME, value, datatype, region'
    )
}}
