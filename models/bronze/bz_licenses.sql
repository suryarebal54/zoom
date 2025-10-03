{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('licenses') }}",
  post_hook="{{ log_audit_end('licenses') }}"
)}}

WITH source_data AS (
  SELECT
    License_ID,
    License_Type,
    Assigned_To_User_ID,
    Start_Date,
    End_Date
  FROM {{ source('zoom', 'licenses') }}
),

final AS (
  SELECT
    License_ID as license_id,
    License_Type as license_type,
    Assigned_To_User_ID as assigned_to_user_id,
    Start_Date as start_date,
    End_Date as end_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
