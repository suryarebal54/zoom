{% macro log_model_start() %}
  {% if execute and this.name != 'bz_audit_log' %}
    {% set query %}
      INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
      SELECT 
        '{{ this.name }}',
        CURRENT_TIMESTAMP(),
        '{{ this.name }}',
        NULL,
        'STARTED'
    {% endset %}
    {% do run_query(query) %}
  {% endif %}
  {{ return('') }}
{% endmacro %}

{% macro log_model_end() %}
  {% if execute and this.name != 'bz_audit_log' %}
    {% set query %}
      INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
      SELECT 
        '{{ this.name }}',
        CURRENT_TIMESTAMP(),
        '{{ this.name }}',
        DATEDIFF('SECOND', (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = '{{ this.name }}' AND status = 'STARTED'), CURRENT_TIMESTAMP()),
        'COMPLETED'
    {% endset %}
    {% do run_query(query) %}
  {% endif %}
  {{ return('') }}
{% endmacro %}
