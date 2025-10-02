{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{{ log_audit_start('support_tickets') }}",
    post_hook="{{ log_audit_end('support_tickets') }}"
)}}

SELECT
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'support_tickets') }}
