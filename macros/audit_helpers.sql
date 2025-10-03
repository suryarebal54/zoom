{% macro log_audit_start(source_table) %}
    {% if this.name != 'bz_audit_log' %}
        insert into {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
        values ('{{ source_table }}', current_timestamp(), '{{ target.user }}', 0, 'STARTED');
    {% endif %}
{% endmacro %}

{% macro log_audit_end(source_table) %}
    {% if this.name != 'bz_audit_log' %}
        insert into {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
        select 
            '{{ source_table }}',
            current_timestamp(),
            '{{ target.user }}',
            datediff('second', max(load_timestamp), current_timestamp()),
            'COMPLETED'
        from {{ ref('bz_audit_log') }}
        where source_table = '{{ source_table }}' and status = 'STARTED';
    {% endif %}
{% endmacro %}
