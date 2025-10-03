{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('participants') }}",
    post_hook="{{ log_audit_end('participants', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    Participant_ID as participant_id,
    Meeting_ID as meeting_id,
    User_ID as user_id,
    Join_Time as join_time,
    Leave_Time as leave_time,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'participants') }}
