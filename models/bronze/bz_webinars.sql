{{config(
    materialized='table',
    pre_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('webinars', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, 'STARTED')
        {% endif %}"
    ],
    post_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('webinars', CURRENT_TIMESTAMP(), CURRENT_USER(), DATEDIFF('SECOND', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'webinars' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED')
        {% endif %}"
    ]
)}}

-- Extract and transform webinars data from raw to bronze layer
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

-- Apply data quality checks and transformations
validated_data AS (
    SELECT
        -- Primary fields
        webinar_id,
        host_id,
        COALESCE(webinar_topic, 'Untitled Webinar') AS webinar_topic,  -- Handle nulls
        start_time,
        end_time,
        COALESCE(registrants, 0) AS registrants,  -- Default to 0 if null
        
        -- Metadata fields
        CURRENT_TIMESTAMP() AS load_timestamp,
        CURRENT_TIMESTAMP() AS update_timestamp,
        'ZOOM_PLATFORM' AS source_system
    FROM source_data
    WHERE webinar_id IS NOT NULL  -- Filter out records with null IDs
      AND host_id IS NOT NULL     -- Ensure host_id is present
)

SELECT * FROM validated_data
