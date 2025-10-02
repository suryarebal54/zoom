{% macro log_model_start(model_name) %}
    {% if model_name != 'bz_audit_log' %}
        INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
        VALUES ('{{ model_name }}', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, 'STARTED')
    {% endif %}
{% endmacro %}

{% macro log_model_end(model_name) %}
    {% if model_name != 'bz_audit_log' %}
        INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
        VALUES ('{{ model_name }}', CURRENT_TIMESTAMP(), CURRENT_USER(), 
                DATEDIFF('SECOND', 
                         (SELECT MAX(load_timestamp) FROM {{ ref('bz_audit_log') }} 
                          WHERE source_table = '{{ model_name }}' AND status = 'STARTED'), 
                         CURRENT_TIMESTAMP()), 
                'COMPLETED')
    {% endif %}
{% endmacro %}
