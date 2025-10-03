{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_licenses') }}",
    post_hook="{{ log_model_end('bz_licenses') }}"
  )
}}

-- Extract and transform licenses data from raw to bronze
select
  license_id,
  license_type,
  assigned_to_user_id,
  start_date,
  end_date,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'licenses') }}
