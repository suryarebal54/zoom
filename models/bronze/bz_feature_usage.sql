{{config(
    materialized='table',
    pre_hook="{{ log_audit_start('feature_usage') }}",
    post_hook="{{ log_audit_end('feature_usage') }}"
)}}

-- Extract and transform feature usage data from source to bronze layer
select
    -- Business data fields
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    
    -- Metadata fields
    current_timestamp() as load_timestamp,
    current_timestamp() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
from {{ source('zoom', 'feature_usage') }}
