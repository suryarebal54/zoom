{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('webinars') }}",
    post_hook="{{ log_audit_end('webinars') }}"
)}}

with source_data as (
    select
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'webinars') }}
)

select
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
