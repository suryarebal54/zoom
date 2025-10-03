{{ config(
    materialized='table',
    schema='bronze'
) }}

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
),

final as (
    select
        -- Direct mappings from source
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
