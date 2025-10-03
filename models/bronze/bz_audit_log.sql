{{ config(
    materialized='incremental',
    unique_key='record_id',
    schema='bronze'
) }}

-- Create the audit log table if it doesn't exist
{% if is_incremental() %}
    select * from {{ this }} where 1=0
{% else %}
    select
        null as record_id,
        'INITIAL_SETUP' as source_table,
        current_timestamp() as load_timestamp,
        '{{ target.user }}' as processed_by,
        0 as processing_time,
        'COMPLETED' as status
    where 1=0
{% endif %}
