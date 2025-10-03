{{config(
  materialized = 'table',
  pre_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('feature_usage', 'STARTED') }} {% endif %}",
  post_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('feature_usage', 'COMPLETED') }} {% endif %}"
)}}

SELECT
  usage_id,
  meeting_id,
  feature_name,
  usage_count,
  usage_date,
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'feature_usage') }}
