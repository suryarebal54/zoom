{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('participants') }}",
  post_hook="{{ log_audit_end('participants') }}"
)}}

WITH source_data AS (
  SELECT
    Participant_ID,
    Meeting_ID,
    User_ID,
    Join_Time,
    Leave_Time
  FROM {{ source('zoom', 'participants') }}
),

final AS (
  SELECT
    Participant_ID as participant_id,
    Meeting_ID as meeting_id,
    User_ID as user_id,
    Join_Time as join_time,
    Leave_Time as leave_time,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
