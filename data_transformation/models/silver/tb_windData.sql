-- depends_on: {{ ref('tb_windData_parsed') }}

{{
    incremental_load_with_hist(
        source_hist_name='historical_data', 
        hist_table='tb_windData_hist', 
        staging_parsed_model='tb_windData_parsed', 
        incremental_column='time',
        columns='time, gust, speed, direction, datatype, region'
    )
}}
