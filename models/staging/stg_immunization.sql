with source as (
    select
        immunization_id,
        patient_reference,
        resource
    from {{ ref('raw_immunization') }}
),

flattened as (
    select
        immunization_id,
        patient_reference,
        resource ->> 'status' as status,
        (resource ->> 'occurrenceDateTime')::timestamptz as occurrence_at,
        (resource -> 'vaccineCode' -> 'coding') as coding_array,
        resource
    from source
),

coding as (
    select
        immunization_id,
        patient_reference,
        status,
        occurrence_at,
        (coding_array -> 0) ->> 'system' as cvx_system,
        (coding_array -> 0) ->> 'code'   as cvx_code,
        (coding_array -> 0) ->> 'display' as vaccine_display,
        resource
    from flattened
)

select * from coding

