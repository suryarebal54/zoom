{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('licenses') }}",
  post_hook="{{ log_audit_end('licenses') }}"
)}}

select
  license_id,
  license_type,
  assigned_to_user_id,
  start_date,
  end_date,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'licenses') }}
