{{
    config(
        materialized='incremental',
        unique_key='record_id'
    )
}}

-- Create the audit log table if it doesn't exist
SELECT
    NULL as record_id,
    NULL as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_USER() as processed_by,
    0 as processing_time,
    'INITIAL' as status
WHERE 1=0
