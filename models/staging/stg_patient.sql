with source as (
    select
        patient_id,
        resource
    from {{ ref('raw_patient') }}
),

flattened as (
    select
        patient_id,
        (resource ->> 'gender')::text as gender,
        (resource ->> 'birthDate')::date as birth_date,
        (resource ->> 'deceasedDateTime')::timestamptz as deceased_at,
        (resource #> '{name,0,family}')::text as family_name,
        (resource #> '{name,0,given,0}')::text as given_name,
        resource
    from source
)

select * from flattened

