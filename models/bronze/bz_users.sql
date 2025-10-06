{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('users') }}",
    post_hook="{{ log_audit_end('users') }}"
)}}

with source_data as (
    select
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'users') }}
)

select
    user_id,
    user_name,
    email,
    company,
    plan_type,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
