{{ config(
  materialized = 'incremental',
  unique_key = 'record_id'
) }}

-- Create the audit log table if it doesn't exist
SELECT
  record_id,
  source_table,
  load_timestamp,
  processed_by,
  processing_time,
  status
FROM {{ this }}
WHERE 1=0
