with cholesterol_observations as (
    select
        o.observation_id,
        split_part(o.patient_reference, '/', 2) as patient_id,
        o.effective_at,
        o.code,
        o.resource
    from {{ ref('stg_observation') }} as o
    where o.code in (
        '2093-3',  -- Cholesterol, total
        '2089-1',  -- Cholesterol in LDL
        '2571-8'   -- Triglycerides (included as part of lipid profile)
    )
),

diagnostic_reports as (
    select
        dr.diagnostic_report_id,
        split_part(dr.patient_reference, '/', 2) as patient_id,
        dr.effective_at,
        dr.code,
        dr.resource
    from {{ ref('stg_diagnostic_report') }} as dr
),

cholesterol_reports as (
    select distinct
        dr.patient_id,
        dr.diagnostic_report_id,
        dr.effective_at,
        dr.code,
        dr.resource
    from diagnostic_reports dr
    join cholesterol_observations co
        on co.patient_id = dr.patient_id
        and co.effective_at::date = dr.effective_at::date
),

ranked as (
    select
        patient_id,
        diagnostic_report_id,
        effective_at,
        code,
        resource,
        row_number() over (
            partition by patient_id
            order by effective_at desc
        ) as rn
    from cholesterol_reports
)

select
    patient_id,
    diagnostic_report_id,
    effective_at,
    code,
    resource
from ranked
where rn = 1

