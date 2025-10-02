{{config(
    materialized='table',
    pre_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('feature_usage', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, 'STARTED')
        {% endif %}"
    ],
    post_hook=[
        "{% if this.name != 'bz_audit_log' %}
            INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
            VALUES ('feature_usage', CURRENT_TIMESTAMP(), CURRENT_USER(), DATEDIFF('SECOND', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = 'feature_usage' AND status = 'STARTED'), CURRENT_TIMESTAMP()), 'COMPLETED')
        {% endif %}"
    ]
)}}

-- Extract and transform feature usage data from raw to bronze layer
WITH source_data AS (
    SELECT
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw', 'feature_usage') }}
),

-- Apply data quality checks and transformations
validated_data AS (
    SELECT
        -- Primary fields
        usage_id,
        meeting_id,
        COALESCE(feature_name, 'Unknown Feature') AS feature_name,  -- Handle nulls
        COALESCE(usage_count, 0) AS usage_count,  -- Default to 0 if null
        usage_date,
        
        -- Metadata fields
        CURRENT_TIMESTAMP() AS load_timestamp,
        CURRENT_TIMESTAMP() AS update_timestamp,
        'ZOOM_PLATFORM' AS source_system
    FROM source_data
    WHERE usage_id IS NOT NULL  -- Filter out records with null IDs
      AND meeting_id IS NOT NULL  -- Ensure meeting_id is present
)

SELECT * FROM validated_data
