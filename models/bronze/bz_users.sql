{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract users data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  user_id,
  user_name,
  email,
  company,
  plan_type,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'users') }}
