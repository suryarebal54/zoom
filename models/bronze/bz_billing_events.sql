{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('billing_events') }}",
    post_hook="{{ log_audit_end('billing_events', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    Event_ID as event_id,
    User_ID as user_id,
    Event_Type as event_type,
    Amount as amount,
    Event_Date as event_date,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'billing_events') }}
