{{ config(
    materialized='table',
    schema='bronze'
) }}

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
),

final as (
    select
        -- Direct mappings from source
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
