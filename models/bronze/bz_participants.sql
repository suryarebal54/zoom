{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('participants') }}",
    post_hook="{{ log_audit_end('participants') }}"
)}}

-- Extract and transform participants data from source to bronze layer
select
    -- Business data fields
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'participants') }}
