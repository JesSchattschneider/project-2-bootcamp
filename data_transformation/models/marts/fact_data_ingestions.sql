Select last_updated, COUNT(FACT_KEY) AS N
from dw.raw_marts.fact_records
group by last_updated
order by last_updated DESC