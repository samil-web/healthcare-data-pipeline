
with patients as (
    select * from {{ ref('stg_patients') }}
),

age_calculated as (
    select
        patient_id,
        gender,
        birth_date,
        date_diff('year', cast(birth_date as date), current_date) as age
    from patients
),

cohorts as (
    select
        *,
        case 
            when age < 18 then 'Pediatric (0-17)'
            when age between 18 and 35 then 'Young Adult (18-35)'
            when age between 35 and 65 then 'Adult (35-65)'
            else 'Senior (65+)'
        end as age_group
    from age_calculated
)

select
    age_group,
    gender,
    count(distinct patient_id) as patient_count
from cohorts
group by 1, 2
order by 1, 2
