{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract feature usage data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  usage_id,
  meeting_id,
  feature_name,
  usage_count,
  usage_date,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'feature_usage') }}
