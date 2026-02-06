{{ config(
    materialized='incremental',
    unique_key='product_key',
    incremental_strategy='merge',
    schema='gold_layer'
) }}

with source_data as(
    select
        product_key,
        default_code,
        name,
        category_name,
        product_type,
        list_price
    from {{ ref('stg_product') }}
)
select
    product_key,
    default_code,
    name,
    category_name,
    product_type,
    list_price
from source_data