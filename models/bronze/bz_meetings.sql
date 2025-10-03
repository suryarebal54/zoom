{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('meetings') }}",
    post_hook="{{ log_audit_end('meetings', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    Meeting_ID as meeting_id,
    Host_ID as host_id,
    Meeting_Topic as meeting_topic,
    Start_Time as start_time,
    End_Time as end_time,
    Duration_Minutes as duration_minutes,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'meetings') }}
