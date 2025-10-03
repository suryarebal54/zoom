{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_support_tickets') }}",
    post_hook="{{ log_model_end('bz_support_tickets') }}"
  )
}}

-- Extract and transform support tickets data from raw to bronze
select
  ticket_id,
  user_id,
  ticket_type,
  resolution_status,
  open_date,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'support_tickets') }}
