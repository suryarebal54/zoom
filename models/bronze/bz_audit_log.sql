{{config(
  materialized = 'incremental',
  unique_key = 'record_id'
)}}

-- Create audit log table if it doesn't exist
SELECT
  ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP()) as record_id,
  'INITIAL_SETUP' as source_table,
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_USER() as processed_by,
  0 as processing_time,
  'SUCCESS' as status
WHERE NOT EXISTS (SELECT 1 FROM {{ this }})
