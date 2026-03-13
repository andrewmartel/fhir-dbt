select * from {{ source('raw', 'diagnostic_report') }}
