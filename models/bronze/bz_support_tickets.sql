{{config(
    materialized='table',
    pre_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('support_tickets', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, 'STARTED')
        {% endif %}"
    ],
    post_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('support_tickets', CURRENT_TIMESTAMP(), CURRENT_USER(), DATEDIFF('SECOND', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'support_tickets' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED')
        {% endif %}"
    ]
)}}

-- Extract and transform support tickets data from raw to bronze layer
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

-- Apply data quality checks and transformations
validated_data AS (
    SELECT
        -- Primary fields
        ticket_id,
        user_id,
        COALESCE(ticket_type, 'Unknown') AS ticket_type,  -- Handle nulls
        COALESCE(resolution_status, 'Open') AS resolution_status,  -- Default to 'Open' if null
        open_date,
        
        -- Metadata fields
        CURRENT_TIMESTAMP() AS load_timestamp,
        CURRENT_TIMESTAMP() AS update_timestamp,
        'ZOOM_PLATFORM' AS source_system
    FROM source_data
    WHERE ticket_id IS NOT NULL  -- Filter out records with null IDs
)

SELECT * FROM validated_data
