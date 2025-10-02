{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('feature_usage') }}",
        post_hook="{{ post_hook_log('feature_usage') }}"
    )
}}

-- Extract and transform feature usage data from raw to bronze
SELECT
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'feature_usage') }}
