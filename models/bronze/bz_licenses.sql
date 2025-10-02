{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract licenses data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  license_id,
  license_type,
  assigned_to_user_id,
  start_date,
  end_date,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'licenses') }}
