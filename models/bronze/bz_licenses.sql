{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('licenses') }}",
    post_hook="{{ log_audit_end('licenses') }}"
)}}

SELECT
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'licenses') }}
