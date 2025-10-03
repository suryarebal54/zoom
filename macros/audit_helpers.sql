{% macro log_audit_start(source_table) %}
    {% if this.name != 'bz_audit_log' %}
        insert into {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, status)
        values ('{{ source_table }}', current_timestamp(), '{{ target.user }}', 'STARTED')
    {% endif %}
{% endmacro %}

{% macro log_audit_end(source_table, start_time) %}
    {% if this.name != 'bz_audit_log' %}
        insert into {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
        values (
            '{{ source_table }}',
            current_timestamp(),
            '{{ target.user }}',
            datediff('second', {{ start_time }}, current_timestamp()),
            'COMPLETED'
        )
    {% endif %}
{% endmacro %}
