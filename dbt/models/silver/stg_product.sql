{{ config(
    materialized='incremental',
    unique_key='product_key',
    incremental_strategy='merge',
    schema='silver_layer'
) }}

with
    source_data as (
        select
            cast(json_value(data, '$.product_key') as int64) as product_key,
            json_value(data, '$.default_code') as default_code,
            json_value(data, '$.name') as name,
            json_value(data, '$.category_name') as category_name,
            json_value(data, '$.product_type') as product_type,
            cast(json_value(data, '$.list_price') as float64) as list_price,
            event_timestamp
        from {{ source('bronze_layer', 'raw_product') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by product_key
            order by event_timestamp desc
        ) as rn
    from source_data
)

select
    product_key,
    default_code,
    name,
    category_name,
    product_type,
    list_price
from ranked
where rn = 1
