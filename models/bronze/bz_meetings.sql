{{config(
    materialized='table',
    schema='bronze'
)}}

WITH source_data AS (
    SELECT
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw', 'meetings') }}
),

validated_data AS (
    SELECT
        -- Primary fields
        COALESCE(meeting_id, 'UNKNOWN') AS meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        
        -- Metadata fields
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
    FROM source_data
)

SELECT * FROM validated_data
