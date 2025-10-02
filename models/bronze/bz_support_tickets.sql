{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('support_tickets') }}",
        post_hook="{{ post_hook_log('support_tickets') }}"
    )
}}

-- Extract and transform support tickets data from raw to bronze
SELECT
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'support_tickets') }}
