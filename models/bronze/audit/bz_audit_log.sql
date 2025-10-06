{{config(
  materialized = 'incremental',
  unique_key = 'record_id'
)}}

-- Create the audit log table if it doesn't exist
with audit_log as (
  {% if is_incremental() %}
    select * from {{ this }}
  {% else %}
    select
      1 as record_id,
      'INITIAL_SETUP' as source_table,
      current_timestamp() as load_timestamp,
      '{{ target.user }}' as processed_by,
      0 as processing_time,
      'COMPLETED' as status
  {% endif %}
)

select * from audit_log
