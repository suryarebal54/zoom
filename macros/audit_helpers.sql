{% macro log_model_start(model_name) %}
  {% if execute %}
    {% set audit_query %}
      INSERT INTO {{ target.schema }}.bz_audit_log (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
      )
      SELECT
        '{{ model_name }}',
        CURRENT_TIMESTAMP(),
        '{{ target.user }}',
        NULL,
        'STARTED'
      ;
    {% endset %}
    {% do run_query(audit_query) %}
  {% endif %}
{% endmacro %}

{% macro log_model_completion(model_name) %}
  {% if execute %}
    {% set audit_query %}
      INSERT INTO {{ target.schema }}.bz_audit_log (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
      )
      SELECT
        '{{ model_name }}',
        CURRENT_TIMESTAMP(),
        '{{ target.user }}',
        DATEDIFF('MILLISECOND', (
          SELECT MAX(load_timestamp)
          FROM {{ target.schema }}.bz_audit_log
          WHERE source_table = '{{ model_name }}'
          AND status = 'STARTED'
        ), CURRENT_TIMESTAMP()),
        'COMPLETED'
      ;
    {% endset %}
    {% do run_query(audit_query) %}
  {% endif %}
{% endmacro %}
