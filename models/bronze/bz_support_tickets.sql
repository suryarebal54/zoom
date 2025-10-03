{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('support_tickets') }}",
  post_hook="{{ log_audit_end('support_tickets') }}"
)}}

WITH source_data AS (
  SELECT
    Ticket_ID,
    User_ID,
    Ticket_Type,
    Resolution_Status,
    Open_Date
  FROM {{ source('zoom', 'support_tickets') }}
),

final AS (
  SELECT
    Ticket_ID as ticket_id,
    User_ID as user_id,
    Ticket_Type as ticket_type,
    Resolution_Status as resolution_status,
    Open_Date as open_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
