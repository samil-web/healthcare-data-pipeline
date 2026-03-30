
with trials as (
    select * from {{ ref('stg_trials') }}
)

select
    status,
    year(start_date) as trial_year,
    count(nct_id) as trial_count
from trials
where start_date is not null
group by 1, 2
order by 3 desc
