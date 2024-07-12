-- depends_on: {{ ref('bop_waterData_parsed') }}
{{
    incremental_load_with_hist(
        source_hist_name='historical_data', 
        hist_table='bop_waterData_hist', 
        staging_parsed_model='bop_waterData_parsed', 
        incremental_column='time',
        columns='time, value, datatype, region'
    )
}}
