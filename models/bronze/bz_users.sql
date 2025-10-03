{{ config(
    materialized='table',
    schema='bronze'
) }}

with source_data as (
    select
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system
    from {{ source('raw', 'users') }}
),

final as (
    select
        -- Direct mappings from source
        user_id,
        user_name,
        email,
        company,
        plan_type,
        -- Metadata columns
        load_timestamp,
        current_timestamp() as update_timestamp,
        coalesce(source_system, 'ZOOM_PLATFORM') as source_system
    from source_data
)

select * from final
