{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('support_tickets') }}",
    post_hook="{{ log_audit_end('support_tickets') }}"
)}}

-- Extract and transform support tickets data from source to bronze layer
select
    -- Business data fields
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'support_tickets') }}
