{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_users') }}",
    post_hook="{{ log_model_end('bz_users') }}"
  )
}}

-- Extract and transform users data from raw to bronze
select
  user_id,
  user_name,
  email,
  company,
  plan_type,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'users') }}
