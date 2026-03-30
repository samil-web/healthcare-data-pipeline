
with source as (
    -- Direct read from Delta/Parquet using glob
    select * from read_parquet('../data/silver/patients/year=*/month=*/*.parquet', hive_partitioning=1)
)

select
    patient_id,
    first_name,
    last_name,
    gender,
    birth_date,
    year as birth_year,
    month as birth_month,
    ingestion_timestamp as processed_at
from source
