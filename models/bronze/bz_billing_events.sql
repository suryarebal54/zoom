{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('billing_events') }}",
        post_hook="{{ post_hook_log('billing_events') }}"
    )
}}

-- Extract and transform billing events data from raw to bronze
SELECT
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'billing_events') }}
