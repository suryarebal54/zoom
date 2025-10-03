{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('feature_usage') }}",
  post_hook="{{ log_audit_end('feature_usage') }}"
)}}

WITH source_data AS (
  SELECT
    Usage_ID,
    Meeting_ID,
    Feature_Name,
    Usage_Count,
    Usage_Date
  FROM {{ source('zoom', 'feature_usage') }}
),

final AS (
  SELECT
    Usage_ID as usage_id,
    Meeting_ID as meeting_id,
    Feature_Name as feature_name,
    Usage_Count as usage_count,
    Usage_Date as usage_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
