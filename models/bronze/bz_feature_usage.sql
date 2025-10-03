{{ config(
    materialized='table',
    schema='bronze'
) }}

with source_data as (
    select
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'feature_usage') }}
),

final as (
    select
        -- Direct mappings from source
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
