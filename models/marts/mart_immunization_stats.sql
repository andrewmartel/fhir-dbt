with immunization_enriched as (
    select
        i.immunization_id,
        i.patient_reference,
        split_part(i.patient_reference, '/', 2) as patient_id,
        i.status,
        i.occurrence_at,
        i.cvx_code,
        i.vaccine_display
    from {{ ref('stg_immunization') }} as i
),

influenza_patients as (
    select distinct
        patient_id
    from immunization_enriched
    where
        lower(coalesce(vaccine_display, '')) like '%influenza%'
        or cvx_code = '140'
),

vaccine_counts as (
    select
        coalesce(vaccine_display, cvx_code) as vaccine_name,
        count(*) as administrations
    from immunization_enriched
    group by 1
),

top_5_vaccines as (
    select
        vaccine_name,
        administrations,
        row_number() over (order by administrations desc, vaccine_name) as rn
    from vaccine_counts
)

select
    (select count(*) from influenza_patients) as patients_with_influenza_vaccine,
    jsonb_agg(
        jsonb_build_object(
            'vaccine_name', vaccine_name,
            'administrations', administrations
        ) order by administrations desc, vaccine_name
    ) filter (where rn <= 5) as top_5_vaccines
from top_5_vaccines

