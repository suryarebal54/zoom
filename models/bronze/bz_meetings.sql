{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('meetings') }}",
  post_hook="{{ log_audit_end('meetings') }}"
)}}

select
  meeting_id,
  host_id,
  meeting_topic,
  start_time,
  end_time,
  duration_minutes,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'meetings') }}
