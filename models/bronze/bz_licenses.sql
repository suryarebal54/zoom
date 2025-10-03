{{config(
  materialized = 'table',
  pre_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('licenses', 'STARTED') }} {% endif %}",
  post_hook="{% if this.identifier != 'bz_audit_log' %} {{ log_table_load('licenses', 'COMPLETED') }} {% endif %}"
)}}

SELECT
  license_id,
  license_type,
  assigned_to_user_id,
  start_date,
  end_date,
  CURRENT_TIMESTAMP() as load_timestamp,
  CURRENT_TIMESTAMP() as update_timestamp,
  '{{ var("source_system") }}' as source_system
FROM {{ source('raw', 'licenses') }}
