{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract participants data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  participant_id,
  meeting_id,
  user_id,
  join_time,
  leave_time,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'participants') }}
