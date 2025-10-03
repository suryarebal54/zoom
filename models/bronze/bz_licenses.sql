{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('licenses') }}",
    post_hook="{{ log_audit_end('licenses') }}"
)}}

-- Extract and transform licenses data from source to bronze layer
select
    -- Business data fields
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'licenses') }}
