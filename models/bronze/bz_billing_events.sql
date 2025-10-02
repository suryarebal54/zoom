{{config(
    materialized='table',
    pre_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('billing_events', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, 'STARTED')
        {% endif %}"
    ],
    post_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('billing_events', CURRENT_TIMESTAMP(), CURRENT_USER(), DATEDIFF('SECOND', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'billing_events' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED')
        {% endif %}"
    ]
)}}

-- Extract and transform billing events data from raw to bronze layer
WITH source_data AS (
    SELECT
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw', 'billing_events') }}
),

-- Apply data quality checks and transformations
validated_data AS (
    SELECT
        -- Primary fields
        event_id,
        user_id,
        COALESCE(event_type, 'Unknown') AS event_type,  -- Handle nulls
        COALESCE(amount, 0) AS amount,  -- Default to 0 if null
        event_date,
        
        -- Metadata fields
        CURRENT_TIMESTAMP() AS load_timestamp,
        CURRENT_TIMESTAMP() AS update_timestamp,
        'ZOOM_PLATFORM' AS source_system
    FROM source_data
    WHERE event_id IS NOT NULL  -- Filter out records with null IDs
)

SELECT * FROM validated_data
