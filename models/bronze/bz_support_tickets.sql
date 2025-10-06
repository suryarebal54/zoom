{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('support_tickets') }}",
    post_hook="{{ log_audit_end('support_tickets') }}"
)}}

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
)

select
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
