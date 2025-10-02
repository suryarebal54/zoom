{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('billing_events') }}",
    post_hook="{{ log_audit_end('billing_events') }}"
)}}

SELECT
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'billing_events') }}
