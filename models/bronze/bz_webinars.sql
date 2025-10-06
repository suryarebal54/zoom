{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('webinars') }}",
  post_hook="{{ log_audit_end('webinars') }}"
)}}

select
  webinar_id,
  host_id,
  webinar_topic,
  start_time,
  end_time,
  registrants,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'webinars') }}
