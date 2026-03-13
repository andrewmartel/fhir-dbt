with source as (
    select
        diagnostic_report_id,
        patient_reference,
        code,
        resource
    from {{ ref('raw_diagnostic_report') }}
),

flattened as (
    select
        diagnostic_report_id,
        patient_reference,
        code,
        (resource ->> 'status') as status,
        (resource ->> 'effectiveDateTime')::timestamptz as effective_at,
        resource
    from source
)

select * from flattened

