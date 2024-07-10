{{
    incremental_load_with_hist(
        source_hist_name='historical_data', 
        hist_table='bop_windData_hist', 
        staging_parsed_model='bop_windData_parsed', 
        incremental_column='TIME',
        columns='TIME, gust, speed, direction, datatype, region'
    )
}}
