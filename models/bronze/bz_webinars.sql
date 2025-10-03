{{config(
  materialized = 'table',
  pre_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('webinars', 'STARTED') }} {% endif %}",
  post_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('webinars', 'COMPLETED') }} {% endif %}"
)}}

SELECT
  webinar_id,
  host_id,
  webinar_topic,
  start_time,
  end_time,
  registrants,
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'webinars') }}
