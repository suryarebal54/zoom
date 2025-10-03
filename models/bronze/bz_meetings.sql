{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('meetings') }}",
  post_hook="{{ log_audit_end('meetings') }}"
)}}

WITH source_data AS (
  SELECT
    Meeting_ID,
    Host_ID,
    Meeting_Topic,
    Start_Time,
    End_Time,
    Duration_Minutes
  FROM {{ source('zoom', 'meetings') }}
),

final AS (
  SELECT
    Meeting_ID as meeting_id,
    Host_ID as host_id,
    Meeting_Topic as meeting_topic,
    Start_Time as start_time,
    End_Time as end_time,
    Duration_Minutes as duration_minutes,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
