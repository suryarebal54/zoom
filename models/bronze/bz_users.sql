{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('users') }}",
  post_hook="{{ log_audit_end('users') }}"
)}}

WITH source_data AS (
  SELECT
    User_ID,
    User_Name,
    Email,
    Company,
    Plan_Type
  FROM {{ source('zoom', 'users') }}
),

final AS (
  SELECT
    User_ID as user_id,
    User_Name as user_name,
    Email as email,
    Company as company,
    Plan_Type as plan_type,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
  FROM source_data
)

SELECT * FROM final
