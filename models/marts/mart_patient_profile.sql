with patients as (
    select
        patient_id,
        gender,
        birth_date,
        deceased_at,
        family_name,
        given_name,
        resource
    from {{ ref('stg_patient') }}
),

immunizations as (
    select
        split_part(patient_reference, '/', 2) as patient_id,
        count(*) as immunization_count
    from {{ ref('stg_immunization') }}
    group by 1
),

vital_sign_counts as (
    select
        split_part(patient_reference, '/', 2) as patient_id,
        count(*) as vital_sign_observation_count
    from {{ ref('stg_observation') }}
    where category = 'vital-signs'
    group by 1
)

select
    p.patient_id,
    p.gender,
    p.birth_date,
    p.deceased_at,
    p.family_name,
    p.given_name,
    coalesce(i.immunization_count, 0) as immunization_count,
    coalesce(v.vital_sign_observation_count, 0) as vital_sign_observation_count
from patients p
left join immunizations i using (patient_id)
left join vital_sign_counts v using (patient_id)

