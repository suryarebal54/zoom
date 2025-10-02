{{config(
    materialized='table',
    pre_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_start(model.name) }} {% endif %}",
    post_hook="{% if model.name != 'bz_audit_log' %} {{ log_model_completion(model.name) }} {% endif %}"
)}}

SELECT
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'licenses') }}
