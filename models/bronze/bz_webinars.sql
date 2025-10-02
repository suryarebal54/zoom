{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract webinars data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  webinar_id,
  host_id,
  webinar_topic,
  start_time,
  end_time,
  registrants,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'webinars') }}
