{{config(
    materialized='table',
    pre_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_start(model.name) }} {% endif %}",
    post_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_completion(model.name) }} {% endif %}"
)}}

SELECT
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'participants') }}
