with source as (
    select
        observation_id,
        patient_reference,
        category,
        code,
        resource
    from {{ ref('raw_observation') }}
),

flattened as (
    select
        observation_id,
        patient_reference,
        category,
        code,
        (resource ->> 'status') as status,
        (resource ->> 'effectiveDateTime')::timestamptz as effective_at,
        resource
    from source
)

select * from flattened

