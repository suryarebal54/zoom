{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('licenses') }}",
    post_hook="{{ log_audit_end('licenses') }}"
)}}

with source_data as (
    select
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'licenses') }}
)

select
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    coalesce(load_timestamp, current_timestamp()) as load_timestamp,
    coalesce(update_timestamp, current_timestamp()) as update_timestamp,
    coalesce(source_system, 'ZOOM_PLATFORM') as source_system
from source_data
