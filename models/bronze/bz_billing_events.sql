{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('billing_events') }}",
  post_hook="{{ log_audit_end('billing_events') }}"
)}}

WITH source_data AS (
  SELECT
    Event_ID,
    User_ID,
    Event_Type,
    Amount,
    Event_Date
  FROM {{ source('zoom', 'billing_events') }}
),

final AS (
  SELECT
    Event_ID as event_id,
    User_ID as user_id,
    Event_Type as event_type,
    Amount as amount,
    Event_Date as event_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
