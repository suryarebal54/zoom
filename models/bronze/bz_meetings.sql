{{config(
  materialized = 'table',
  pre_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('meetings', 'STARTED') }} {% endif %}",
  post_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('meetings', 'COMPLETED') }} {% endif %}"
)}}

SELECT
  meeting_id,
  host_id,
  meeting_topic,
  start_time,
  end_time,
  duration_minutes,
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'meetings') }}
