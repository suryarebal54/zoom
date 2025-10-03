{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_webinars') }}",
    post_hook="{{ log_model_end('bz_webinars') }}"
  )
}}

-- Extract and transform webinars data from raw to bronze
select
  webinar_id,
  host_id,
  webinar_topic,
  start_time,
  end_time,
  registrants,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'webinars') }}
