{{config(
    materialized='incremental',
    unique_key='record_id'
)}}

-- This model is created first to support audit logging
-- It will be used by pre-hooks and post-hooks to log model execution

SELECT
    record_id,
    source_table,
    load_timestamp,
    processed_by,
    processing_time,
    status
FROM {{ target.schema }}.bz_audit_log

{% if is_incremental() %}
WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}
