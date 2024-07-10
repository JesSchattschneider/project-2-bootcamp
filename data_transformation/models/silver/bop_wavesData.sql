{{
    incremental_load_with_hist(
        source_hist_name='historical_data', 
        hist_table='bop_wavesData_hist', 
        staging_parsed_model='bop_wavesData_parsed', 
        incremental_column='time',
        columns='time, SIGNIFICANTHEIGHT, MAXHEIGHT, DIRECTION, datatype, region'
    )
}}
