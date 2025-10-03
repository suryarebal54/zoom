{{ config(
    materialized='table',
    schema='bronze'
) }}

with source_data as (
    select
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'support_tickets') }}
),

final as (
    select
        -- Direct mappings from source
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
