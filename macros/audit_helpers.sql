{% macro log_table_load(source_table, status) %}
    INSERT INTO {{ ref('bz_audit_log') }} (source_table, load_timestamp, processed_by, processing_time, status)
    SELECT 
        '{{ source_table }}' as source_table,
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_USER() as processed_by,
        DATEDIFF('MILLISECOND', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()) as processing_time,
        '{{ status }}' as status
{% endmacro %}

{% macro pre_hook_log(source_table) %}
    {% if this.name != 'bz_audit_log' %}
        {{ log_table_load(source_table, 'STARTED') }}
    {% endif %}
{% endmacro %}

{% macro post_hook_log(source_table) %}
    {% if this.name != 'bz_audit_log' %}
        {{ log_table_load(source_table, 'COMPLETED') }}
    {% endif %}
{% endmacro %}
