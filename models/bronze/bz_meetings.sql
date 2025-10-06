{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('meetings') }}",
    post_hook="{{ log_audit_end('meetings') }}"
)}}

with source_data as (
    select
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'meetings') }}
)

select
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
