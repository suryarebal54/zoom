{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('meetings') }}",
        post_hook="{{ post_hook_log('meetings') }}"
    )
}}

-- Extract and transform meetings data from raw to bronze
SELECT
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'meetings') }}
