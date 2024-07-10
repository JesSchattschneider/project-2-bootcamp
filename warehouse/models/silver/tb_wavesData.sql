{{
    incremental_load_with_hist(
        source_hist_name='historical_data', 
        hist_table='tb_wavesData_hist', 
        staging_parsed_model='tb_wavesData_parsed', 
        incremental_column='time',
        columns='time, SIGNIFICANTHEIGHT, MAXHEIGHT, DIRECTION, datatype, region'
    )
}}
