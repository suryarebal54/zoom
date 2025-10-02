{{config(
  materialized = 'incremental',
  unique_key = 'record_id'
)}}

-- Create the audit log table if it doesn't exist
SELECT
  ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP()) as record_id,
  'INITIAL' as source_table,
  CURRENT_TIMESTAMP() as load_timestamp,
  'SYSTEM' as processed_by,
  0 as processing_time,
  'INITIALIZED' as status

{% if is_incremental() %}
  -- This will only be executed on subsequent runs
  WHERE FALSE
{% endif %}
