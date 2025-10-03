{{ config(
    materialized='table',
    schema='bronze'
) }}

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
),

final as (
    select
        -- Direct mappings from source
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
