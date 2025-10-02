{{ config(
    materialized='table',
    schema='bronze'
) }}

WITH source_data AS (
    SELECT
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw', 'support_tickets') }}
),

validated_data AS (
    SELECT
        -- Primary fields
        TRIM(ticket_id) AS ticket_id,
        TRIM(user_id) AS user_id,
        TRIM(ticket_type) AS ticket_type,
        TRIM(resolution_status) AS resolution_status,
        open_date,
        
        -- Metadata fields
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
        COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
    FROM source_data
)

SELECT * FROM validated_data
