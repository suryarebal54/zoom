{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_feature_usage') }}",
    post_hook="{{ log_model_end('bz_feature_usage') }}"
  )
}}

-- Extract and transform feature usage data from raw to bronze
select
  usage_id,
  meeting_id,
  feature_name,
  usage_count,
  usage_date,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'feature_usage') }}
