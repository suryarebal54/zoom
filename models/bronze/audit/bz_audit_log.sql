{{config(
    materialized='incremental',
    unique_key='record_id'
)}}

-- Create the audit log table if it doesn't exist
with audit_log as (
    {% if is_incremental() %}
        select * from {{ this }}
    {% else %}
        select
            null::number as record_id,
            null::string as source_table,
            null::timestamp_ntz as load_timestamp,
            null::string as processed_by,
            null::number as processing_time,
            null::string as status
        where 1=0
    {% endif %}
)

select * from audit_log
