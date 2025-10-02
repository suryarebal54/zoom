{{config(
    materialized='incremental',
    unique_key='record_id'
)}}

-- Create a table to track the execution of bronze models
WITH source_data AS (
    SELECT
        {{ dbt_utils.surrogate_key(['source_table', 'load_timestamp', 'status']) }} as record_id,
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
    FROM {{ this }}
    
    {% if is_incremental() %}
        -- Only get new records if this is an incremental run
        WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source_data

-- Initial run will have no data, so we need to ensure we have a valid structure
{% if not is_incremental() %}
WHERE 1=0
{% endif %}
