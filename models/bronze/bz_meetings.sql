{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('meetings') }}",
    post_hook="{{ log_audit_end('meetings') }}"
)}}

-- Extract and transform meetings data from source to bronze layer
select
    -- Business data fields
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'meetings') }}
