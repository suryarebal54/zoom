{% macro audit_start_hook() %}
  {% if this.name != 'bz_audit_log' %}
    {% set query %}
      INSERT INTO {{ ref('bz_audit_log') }} (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
      )
      SELECT
        '{{ this.name }}',
        CURRENT_TIMESTAMP(),
        CURRENT_USER(),
        NULL,
        'STARTED'
      {% endset %}
      {% do run_query(query) %}
  {% endif %}
{% endmacro %}

{% macro audit_end_hook() %}
  {% if this.name != 'bz_audit_log' %}
    {% set query %}
      INSERT INTO {{ ref('bz_audit_log') }} (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
      )
      SELECT
        '{{ this.name }}',
        CURRENT_TIMESTAMP(),
        CURRENT_USER(),
        DATEDIFF('SECOND', (
          SELECT MAX(load_timestamp)
          FROM {{ ref('bz_audit_log') }}
          WHERE source_table = '{{ this.name }}'
          AND status = 'STARTED'
        ), CURRENT_TIMESTAMP()),
        'COMPLETED'
      {% endset %}
      {% do run_query(query) %}
  {% endif %}
{% endmacro %}

{% macro error_handler() %}
  {% if execute %}
    {% do exceptions.warn("Error occurred during model execution: " ~ this.name) %}
    {% set query %}
      INSERT INTO {{ ref('bz_audit_log') }} (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
      )
      SELECT
        '{{ this.name }}',
        CURRENT_TIMESTAMP(),
        CURRENT_USER(),
        NULL,
        'FAILED'
      {% endset %}
      {% do run_query(query) %}
  {% endif %}
{% endmacro %}
