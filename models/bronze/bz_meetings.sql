{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_meetings') }}",
    post_hook="{{ log_model_end('bz_meetings') }}"
  )
}}

-- Extract and transform meetings data from raw to bronze
select
  meeting_id,
  host_id,
  meeting_topic,
  start_time,
  end_time,
  duration_minutes,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'meetings') }}
