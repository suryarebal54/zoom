{{config(
  materialized = 'table',
  pre_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('billing_events', 'STARTED') }} {% endif %}",
  post_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('billing_events', 'COMPLETED') }} {% endif %}"
)}}

SELECT
  event_id,
  user_id,
  event_type,
  amount,
  event_date,
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'billing_events') }}
