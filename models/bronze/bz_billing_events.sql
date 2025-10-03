{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('billing_events') }}",
    post_hook="{{ log_audit_end('billing_events') }}"
)}}

-- Extract and transform billing events data from source to bronze layer
select
    -- Business data fields
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'billing_events') }}
