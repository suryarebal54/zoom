{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('participants') }}",
    post_hook="{{ log_audit_end('participants') }}"
)}}

with source_data as (
    select
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'participants') }}
)

select
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
