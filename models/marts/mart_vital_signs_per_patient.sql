with vitals as (
    select
        split_part(patient_reference, '/', 2) as patient_id,
        observation_id
    from {{ ref('stg_observation') }}
    where category = 'vital-signs'
),

per_patient as (
    select
        patient_id,
        count(*) as vital_sign_observation_count
    from vitals
    group by 1
),

overall as (
    select
        avg(vital_sign_observation_count)::numeric(10,2) as avg_vital_signs_per_patient
    from per_patient
)

select * from overall

