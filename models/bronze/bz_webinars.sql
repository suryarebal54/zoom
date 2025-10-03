{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('webinars') }}",
    post_hook="{{ log_audit_end('webinars') }}"
)}}

-- Extract and transform webinars data from source to bronze layer
select
    -- Business data fields
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'webinars') }}
