{{config(
    materialized='table',
    pre_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_start(model.name) }} {% endif %}",
    post_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_completion(model.name) }} {% endif %}"
)}}

SELECT
    user_id,
    user_name,
    email,
    company,
    plan_type,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'users') }}
