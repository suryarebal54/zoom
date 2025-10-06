{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('billing_events') }}",
    post_hook="{{ log_audit_end('billing_events') }}"
)}}

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
)

select
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
