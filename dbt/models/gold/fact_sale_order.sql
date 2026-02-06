{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='gold_layer'
) }}

with source_data as(
    select id ,
    order_id,
    customer_id,
    salesperson_id,
    product_id,
    order_date,
    quantity,
    price_unit,
    discount_pct,
    subtotal_amount,
    total_amount,
    order_payment_state
    from {{ ref('stg_sale_order') }}
)
select
    id,
    order_id,
    customer_id,
    salesperson_id,
    product_id,
    order_date,
    quantity,
    price_unit,
    discount_pct,
    subtotal_amount,
    total_amount,
    order_payment_state
from source_data