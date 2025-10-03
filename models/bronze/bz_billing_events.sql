{{
  config(
    materialized = 'table',
    pre_hook="{{ log_model_start('bz_billing_events') }}",
    post_hook="{{ log_model_end('bz_billing_events') }}"
  )
}}

-- Extract and transform billing events data from raw to bronze
select
  event_id,
  user_id,
  event_type,
  amount,
  event_date,
  current_timestamp() as load_timestamp,
  current_timestamp() as update_timestamp,
  'ZOOM_PLATFORM' as source_system
from {{ source('raw', 'billing_events') }}
