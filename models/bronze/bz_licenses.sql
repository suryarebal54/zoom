{{  
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('licenses') }}",
        post_hook="{{ post_hook_log('licenses') }}"
    )
}}

-- Extract and transform licenses data from raw to bronze
SELECT
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'licenses') }}
