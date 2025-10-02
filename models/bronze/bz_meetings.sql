{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract meetings data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  meeting_id,
  host_id,
  meeting_topic,
  start_time,
  end_time,
  duration_minutes,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'meetings') }}
