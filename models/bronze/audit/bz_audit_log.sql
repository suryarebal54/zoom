{{config(
  materialized = 'incremental',
  unique_key = 'record_id'
)}}

-- Create the audit log table if it doesn't exist
SELECT
  COALESCE(record_id, 0) as record_id,
  source_table,
  load_timestamp,
  processed_by,
  processing_time,
  status
FROM {{ source('zoom', 'audit_log') }}
WHERE 1=0  -- Initial empty table
