{{config(
  materialized = 'table',
  schema = 'bronze'
)}}

WITH source_data AS (
  SELECT
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    update_timestamp,
    source_system
  FROM {{ source('raw', 'users') }}
),

validated_data AS (
  SELECT
    -- Primary fields
    user_id,
    user_name,
    email,
    company,
    plan_type,
    -- Metadata fields
    load_timestamp,
    update_timestamp,
    source_system
  FROM source_data
)

SELECT
  user_id,
  user_name,
  email,
  company,
  plan_type,
  COALESCE(load_timestamp, CURRENT_TIMESTAMP()) AS load_timestamp,
  COALESCE(update_timestamp, CURRENT_TIMESTAMP()) AS update_timestamp,
  COALESCE(source_system, 'ZOOM_PLATFORM') AS source_system
FROM validated_data
