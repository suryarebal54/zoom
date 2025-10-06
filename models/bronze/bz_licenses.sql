{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

WITH source_data AS (
  SELECT
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    load_timestamp,
    update_timestamp,
    source_system
  FROM {{ source('raw', 'licenses') }}
),

validated_data AS (
  SELECT
    -- Primary fields
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    -- Metadata fields
    load_timestamp,
    update_timestamp,
    source_system
  FROM source_data
)

SELECT
  license_id,
  license_type,
  assigned_to_user_id,
  start_date,
  end_date,
  COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
  COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
  COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
FROM validated_data
