
with source as (
    -- Read from Silver Claims Delta/Parquet files
    -- Use specific glob to avoid manifest files and handle hive partitioning
    select * from read_parquet('../data/silver/hospital_claims/year=*/month=*/*.parquet', hive_partitioning=1)
)

select
    facility_id,
    facility_name,
    period,
    standardized_date as service_date,
    avg_spending_hospital,
    avg_spending_state,
    avg_spending_national,
    year,
    month,
    ingestion_timestamp as processed_at
from source
where facility_id is not null
  and avg_spending_hospital > 0
