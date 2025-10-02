{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('users') }}",
    post_hook="{{ log_audit_end('users') }}"
)}}

SELECT
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'users') }}
