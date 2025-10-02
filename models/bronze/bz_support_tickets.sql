{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

-- Extract support tickets data from raw source and apply transformations
SELECT
  -- Direct mappings from source
  ticket_id,
  user_id,
  ticket_type,
  resolution_status,
  open_date,
  -- Metadata columns
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'support_tickets') }}
