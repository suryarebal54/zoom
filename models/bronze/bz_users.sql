{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('users') }}",
  post_hook="{{ log_audit_end('users') }}"
)}}

select
  user_id,
  user_name,
  email,
  company,
  plan_type,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'users') }}
