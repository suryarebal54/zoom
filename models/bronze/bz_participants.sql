{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_participants') }}",
    post_hook="{{ log_model_end('bz_participants') }}"
  )
}}

-- Extract and transform participants data from raw to bronze
select
  participant_id,
  meeting_id,
  user_id,
  join_time,
  leave_time,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'participants') }}
