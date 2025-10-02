{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('webinars') }}",
    post_hook="{{ log_audit_end('webinars') }}"
)}}

SELECT
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'webinars') }}
