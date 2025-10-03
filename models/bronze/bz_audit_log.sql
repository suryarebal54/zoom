{{config(
    materialized='incremental',
    unique_key='record_id'
)}}

-- Create the audit log table if it doesn't exist
CREATE TABLE IF NOT EXISTS {{ this }} (
    record_id NUMBER AUTOINCREMENT,
    source_table STRING,
    load_timestamp TIMESTAMP_NTZ,
    processed_by STRING,
    processing_time NUMBER,
    status STRING
);

-- For incremental runs, we don't need to select anything
{% if is_incremental() %}
    SELECT
        record_id,
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
    FROM {{ this }}
    WHERE 1=0
{% else %}
    -- For the initial run, create an empty result set
    SELECT
        CAST(NULL AS NUMBER) AS record_id,
        CAST(NULL AS STRING) AS source_table,
        CAST(NULL AS TIMESTAMP_NTZ) AS load_timestamp,
        CAST(NULL AS STRING) AS processed_by,
        CAST(NULL AS NUMBER) AS processing_time,
        CAST(NULL AS STRING) AS status
    WHERE 1=0
{% endif %}
