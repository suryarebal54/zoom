{{ config(
    materialized='incremental',
    unique_key='record_id',
    schema='bronze'
) }}

-- Create the audit log table if it doesn't exist
WITH audit_log AS (
    SELECT
        CASE WHEN '{{ invocation_id }}' = 'None' THEN NULL ELSE '{{ invocation_id }}' END AS record_id,
        'INITIAL_SETUP' AS source_table,
        CURRENT_TIMESTAMP() AS load_timestamp,
        CURRENT_USER() AS processed_by,
        0 AS processing_time,
        'INITIALIZED' AS status
    {% if is_incremental() %}
    WHERE FALSE  -- Don't insert this record if the table already exists
    {% endif %}
)

SELECT * FROM audit_log
