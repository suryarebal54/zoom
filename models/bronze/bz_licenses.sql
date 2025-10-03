{{ config(
    materialized='table',
    schema='bronze'
) }}

with source_data as (
    select
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'licenses') }}
),

final as (
    select
        -- Direct mappings from source
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
