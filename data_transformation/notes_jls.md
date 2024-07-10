- save historical data from orginal db to csvs (SQL)
- process historical data and get them in the same format that should be returned fromt the API (+ region and datatype) - used a jupyter notebook for this step
- save processed historical data as raw tables in dbt (save results in the step above as csvs in the seeds folder and run `dbt seed`)
- create models and run `dbt run`

- dim_wind is skipped because the table it depends on failed to be generated (no data returned in the API)