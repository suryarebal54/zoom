{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('participants') }}",
    post_hook="{{ log_audit_end('participants') }}"
)}}

SELECT
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'participants') }}
