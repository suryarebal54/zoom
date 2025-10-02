{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('feature_usage') }}",
    post_hook="{{ log_audit_end('feature_usage') }}"
)}}

SELECT
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'feature_usage') }}
