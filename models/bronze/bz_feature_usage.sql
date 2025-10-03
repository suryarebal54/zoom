{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('feature_usage') }}",
    post_hook="{{ log_audit_end('feature_usage', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    Usage_ID as usage_id,
    Meeting_ID as meeting_id,
    Feature_Name as feature_name,
    Usage_Count as usage_count,
    Usage_Date as usage_date,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'feature_usage') }}
