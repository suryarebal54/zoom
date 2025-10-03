{% macro log_model_start(model_name) %}
  {% if model_name != 'bz_audit_log' %}
    insert into {{ ref('bz_audit_log') }} (
      source_table, 
      load_timestamp, 
      processed_by, 
      processing_time, 
      status
    ) 
    values (
      '{{ model_name }}',
      current_timestamp(),
      '{{ target.user }}',
      null,
      'STARTED'
    )
  {% endif %}
{% endmacro %}

{% macro log_model_end(model_name) %}
  {% if model_name != 'bz_audit_log' %}
    insert into {{ ref('bz_audit_log') }} (
      source_table, 
      load_timestamp, 
      processed_by, 
      processing_time, 
      status
    ) 
    select 
      '{{ model_name }}',
      current_timestamp(),
      '{{ target.user }}',
      datediff('millisecond', max(load_timestamp), current_timestamp()) / 1000.0,
      'COMPLETED'
    from {{ ref('bz_audit_log') }}
    where source_table = '{{ model_name }}'
    and status = 'STARTED'
    order by load_timestamp desc
    limit 1
  {% endif %}
{% endmacro %}
