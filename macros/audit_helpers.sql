{% macro log_table_load(source_table, status) %}
  {% if execute %}
    {% set query %}
      INSERT INTO {{ ref('bz_audit_log') }} (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
      )
      SELECT
        '{{ source_table }}',
        CURRENT_TIMESTAMP(),
        CURRENT_USER(),
        DATEDIFF('MILLISECOND', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()) / 1000.0,
        '{{ status }}'
    {% endset %}
    {% do run_query(query) %}
  {% endif %}
{% endmacro %}
