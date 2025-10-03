{{ config(
    materialized='table',
    schema='bronze'
) }}

with source_data as (
    select
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'billing_events') }}
),

final as (
    select
        -- Direct mappings from source
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
