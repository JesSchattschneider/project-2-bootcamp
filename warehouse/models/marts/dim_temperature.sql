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

with tb_waterdata as (
    select
        time,
        region,
        value as temperature
    from {{ ref('tb_waterData') }}
    {% if is_incremental() %}
    where time > {{ max_time }}
    {% endif %}
),
bop_waterdata as (
    select
        time,
        region,
        value as temperature
    from {{ ref('bop_waterData') }}
    {% if is_incremental() %}
    where time > {{ max_time }}
    {% endif %}
),
combined_data as (
    select * from tb_waterdata
    union all
    select * from bop_waterdata
)

select 
    {{ dbt_utils.generate_surrogate_key(['time', 'region']) }} as temperature_key,
    time,
    region,
    temperature,
    current_timestamp() as last_updated
from combined_data
