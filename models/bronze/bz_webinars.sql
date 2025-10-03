{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('webinars') }}",
    post_hook="{{ log_audit_end('webinars', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    Webinar_ID as webinar_id,
    Host_ID as host_id,
    Webinar_Topic as webinar_topic,
    Start_Time as start_time,
    End_Time as end_time,
    Registrants as registrants,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'webinars') }}
