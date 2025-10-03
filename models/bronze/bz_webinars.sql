{{config(
    materialized='table',
    schema='bronze'
)}}

WITH source_data AS (
    SELECT
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw', 'webinars') }}
),

validated_data AS (
    SELECT
        -- Primary fields
        COALESCE(webinar_id, 'UNKNOWN') AS webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        
        -- Metadata fields
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
    FROM source_data
)

SELECT * FROM validated_data
