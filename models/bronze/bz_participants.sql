{{ config(
    materialized='table',
    schema='bronze'
) }}

WITH source_data AS (
    SELECT
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw', 'participants') }}
),

validated_data AS (
    SELECT
        -- Primary fields
        TRIM(participant_id) AS participant_id,
        TRIM(meeting_id) AS meeting_id,
        TRIM(user_id) AS user_id,
        join_time,
        leave_time,
        
        -- Metadata fields
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
    FROM source_data
)

SELECT * FROM validated_data
