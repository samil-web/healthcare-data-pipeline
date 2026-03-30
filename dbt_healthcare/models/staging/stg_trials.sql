
with source as (
    -- Read from Silver Trials Parquet files
    select * from read_parquet('../data/silver/clinical_trials/*.parquet')
)

select
    nct_id,
    start_date as start_date_raw,
    -- Using try_strptime (DuckDB equivalent of try_to_date)
    coalesce(
        try_strptime(start_date_raw, '%Y-%m-%d'), 
        try_strptime(start_date_raw, '%Y-%m'), 
        try_strptime(start_date_raw, '%B %Y')
    ) as start_date,
    'Unknown' as status -- Mocking status since it wasn't in the simplified silver version
from source
