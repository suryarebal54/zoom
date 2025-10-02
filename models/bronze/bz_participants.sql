{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('participants') }}",
        post_hook="{{ post_hook_log('participants') }}"
    )
}}

-- Extract and transform participants data from raw to bronze
SELECT
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'participants') }}
