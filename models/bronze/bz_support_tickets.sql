{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('support_tickets') }}",
  post_hook="{{ log_audit_end('support_tickets') }}"
)}}

select
  ticket_id,
  user_id,
  ticket_type,
  resolution_status,
  open_date,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'support_tickets') }}
