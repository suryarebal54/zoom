{{
  config(
    materialized = 'incremental',
    unique_key = 'record_id'
  )
}}

-- Create the audit log table if it doesn't exist
{% if is_incremental() %}
  select * from {{ this }}
  where 1 = 0  -- No records to add in incremental mode
{% else %}
  select
    null as record_id,  -- This will be auto-incremented
    cast(null as string) as source_table,
    cast(null as timestamp_ntz) as load_timestamp,
    cast(null as string) as processed_by,
    cast(null as number) as processing_time,
    cast(null as string) as status
  where 1 = 0  -- Create empty table with correct schema
{% endif %}
