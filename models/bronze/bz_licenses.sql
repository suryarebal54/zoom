{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('licenses') }}",
    post_hook="{{ log_audit_end('licenses', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    License_ID as license_id,
    License_Type as license_type,
    Assigned_To_User_ID as assigned_to_user_id,
    Start_Date as start_date,
    End_Date as end_date,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'licenses') }}
