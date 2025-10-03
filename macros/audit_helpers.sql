{% macro log_audit_start(source_table) %}
  {% if execute and not this.name == 'bz_audit_log' %}
    INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status)
    VALUES ('{{ source_table }}', CURRENT_TIMESTAMP(), '{{ target.user }}', 'STARTED');
  {% endif %}
{% endmacro %}

{% macro log_audit_end(source_table) %}
  {% if execute and not this.name == 'bz_audit_log' %}
    INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
    VALUES (
      '{{ source_table }}', 
      CURRENT_TIMESTAMP(), 
      '{{ target.user }}', 
      DATEDIFF(SECOND, (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} WHERE source_table = '{{ source_table }}' AND status = 'STARTED'), CURRENT_TIMESTAMP()),
      'COMPLETED'
    );
  {% endif %}
{% endmacro %}
