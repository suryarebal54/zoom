{{config(
    materialized='table',
    pre_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_start(model.name) }} {% endif %}",
    post_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_completion(model.name) }} {% endif %}"
)}}

SELECT
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'meetings') }}
