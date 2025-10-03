{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('users') }}",
    post_hook="{{ log_audit_end('users') }}"
)}}

-- Extract and transform users data from source to bronze layer
select
    -- Business data fields
    user_id,
    user_name,
    email,
    company,
    plan_type,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'users') }}
