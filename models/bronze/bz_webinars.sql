{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('webinars') }}",
        post_hook="{{ post_hook_log('webinars') }}"
    )
}}

-- Extract and transform webinars data from raw to bronze
SELECT
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'webinars') }}
