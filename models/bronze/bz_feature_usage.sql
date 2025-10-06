{{config(
  materialized = 'table',
  pre_hook="{{ log_audit_start('feature_usage') }}",
  post_hook="{{ log_audit_end('feature_usage') }}"
)}}

select
  usage_id,
  meeting_id,
  feature_name,
  usage_count,
  usage_date,
  load_timestamp,
  update_timestamp,
  source_system
from {{ source('raw', 'feature_usage') }}
