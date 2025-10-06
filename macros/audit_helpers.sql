{% macro log_model_start() %}
  {% if this.name != 'bz_audit_log' %}
    insert into {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
    values ('{{ this.name }}', current_timestamp(), '{{ target.user }}', 0, 'STARTED');
  {% endif %}
{% endmacro %}

{% macro log_model_end() %}
  {% if this.name != 'bz_audit_log' %}
    insert into {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
    values ('{{ this.name }}', current_timestamp(), '{{ target.user }}', datediff('second', (select max(load_timestamp) from {{ ref('bz_audit_log') }} where source_table = '{{ this.name }}' and status = 'STARTED'), current_timestamp()), 'COMPLETED');
  {% endif %}
{% endmacro %}

{% macro handle_null_values(column_name, default_value) %}
  coalesce({{ column_name }}, {{ default_value }})
{% endmacro %}
