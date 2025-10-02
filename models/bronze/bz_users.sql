{{
    config(
        materialized='table',
        pre_hook="{{ pre_hook_log('users') }}",
        post_hook="{{ post_hook_log('users') }}"
    )
}}

-- Extract and transform users data from raw to bronze
SELECT
    user_id,
    user_name,
    email,
    company,
    plan_type,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('zoom', 'users') }}
