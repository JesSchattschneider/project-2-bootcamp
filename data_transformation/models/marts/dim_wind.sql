{{ config(
    materialized="incremental",
    incremental_strategy="delete+insert"
) }}

{% set max_time %}
  {% if is_incremental() %}
    (select max(time) from {{ this }})
  {% else %}
    null
  {% endif %}
{% endset %}

with tb_winddata as (
    select
        time,
        region,
        gust,
        speed,
        direction
    from {{ ref('tb_windData') }}
    {% if is_incremental() %}
    where time > {{ max_time }}
    {% endif %}
),
bop_winddata as (
    select
        time,
        region,
        gust,
        speed,
        direction
    from {{ ref('bop_windData') }}
    {% if is_incremental() %}
    where time > {{ max_time }}
    {% endif %}
),
combined_data as (
    select time, region, AVG(gust) as gust, AVG(speed) as speed, AVG(direction) as direction
    from (
        select * from tb_winddata
        union all
        select * from bop_winddata
    ) all_data
    group by time, region
)

select 
    {{ dbt_utils.generate_surrogate_key(['time', 'region']) }} as wind_key,
    time,
    region,
    gust,
    speed,
    direction,
    current_timestamp() as last_updated
from combined_data

