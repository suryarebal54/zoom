{{config(
    materialized='table',
    schema='bronze'
)}}

-- Create the audit log table if it doesn't exist
SELECT
    NULL as record_id,
    'INITIAL_SETUP' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    '{{ target.user }}' as processed_by,
    0 as processing_time,
    'SETUP' as status
WHERE NOT EXISTS (SELECT 1 FROM {{ this }})
