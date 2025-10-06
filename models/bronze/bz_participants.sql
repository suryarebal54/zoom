{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('participants') }}",
  post_hook="{{ log_audit_end('participants') }}"
)}}

select
  participant_id,
  meeting_id,
  user_id,
  join_time,
  leave_time,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'participants') }}
