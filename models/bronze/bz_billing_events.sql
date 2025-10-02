{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract billing events data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  event_id,
  user_id,
  event_type,
  amount,
  event_date,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'billing_events') }}
