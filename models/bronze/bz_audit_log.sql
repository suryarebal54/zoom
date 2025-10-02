{{config(
    materialized='table',
    schema='bronze',
    tags=['audit']
)}}

-- Create the audit log table
SELECT
    NULL as record_id,
    'INITIAL_SETUP' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    '{{ target.user }}' as processed_by,
    0 as processing_time,
    'SETUP' as status
