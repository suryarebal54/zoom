{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('webinars') }}",
  post_hook="{{ log_audit_end('webinars') }}"
)}}

WITH source_data AS (
  SELECT
    Webinar_ID,
    Host_ID,
    Webinar_Topic,
    Start_Time,
    End_Time,
    Registrants
  FROM {{ source('zoom', 'webinars') }}
),

final AS (
  SELECT
    Webinar_ID as webinar_id,
    Host_ID as host_id,
    Webinar_Topic as webinar_topic,
    Start_Time as start_time,
    End_Time as end_time,
    Registrants as registrants,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
