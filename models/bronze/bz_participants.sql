{{config(
    materialized='table',
    pre_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('participants', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, 'STARTED')
        {% endif %}"
    ],
    post_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('participants', CURRENT_TIMESTAMP(), CURRENT_USER(), DATEDIFF('SECOND', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'participants' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED')
        {% endif %}"
    ]
)}}

-- Extract and transform participants data from raw to bronze layer
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

-- Apply data quality checks and transformations
validated_data AS (
    SELECT
        -- Primary fields
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        
        -- Metadata fields
        CURRENT_TIMESTAMP() AS load_timestamp,
        CURRENT_TIMESTAMP() AS update_timestamp,
        'ZOOM_PLATFORM' AS source_system
    FROM source_data
    WHERE participant_id IS NOT NULL  -- Filter out records with null IDs
      AND meeting_id IS NOT NULL      -- Ensure meeting_id is present
)

SELECT * FROM validated_data
