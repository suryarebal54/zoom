{{config(
    materialized='table',
    pre_hook="{% set start_time = modules.datetime.datetime.now() %} {{ log_audit_start('support_tickets') }}",
    post_hook="{{ log_audit_end('support_tickets', 'timestamp\'' + start_time.strftime('%Y-%m-%d %H:%M:%S') + '\'') }}"
)}}

SELECT
    -- Source columns
    Ticket_ID as ticket_id,
    User_ID as user_id,
    Ticket_Type as ticket_type,
    Resolution_Status as resolution_status,
    Open_Date as open_date,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'support_tickets') }}
