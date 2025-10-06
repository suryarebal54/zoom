{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

WITH source_data AS (
  SELECT
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    load_timestamp,
    update_timestamp,
    source_system
  FROM {{ source('raw', 'billing_events') }}
),

validated_data AS (
  SELECT
    -- Primary fields
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    -- Metadata fields
    load_timestamp,
    update_timestamp,
    source_system
  FROM source_data
)

SELECT
  event_id,
  user_id,
  event_type,
  amount,
  event_date,
  COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
  COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
  COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
FROM validated_data
