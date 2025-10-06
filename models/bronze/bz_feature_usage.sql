{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

WITH source_data AS (
  SELECT
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system
  FROM {{ source('raw', 'feature_usage') }}
),

validated_data AS (
  SELECT
    -- Primary fields
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    -- Metadata fields
    load_timestamp,
    update_timestamp,
    source_system
  FROM source_data
)

SELECT
  usage_id,
  meeting_id,
  feature_name,
  usage_count,
  usage_date,
  COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
  COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
  COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
FROM validated_data
