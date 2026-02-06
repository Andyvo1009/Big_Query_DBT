{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='gold_layer'
) }}

with source_data as(
    select id ,
    name,
    LOWER(email) AS email,
    phone,
    stage_name,
    active,
    salesperson_id,
    customer_id,
    expected_revenue,
    priority,
    create_date,
    write_date,
    is_won
    from {{ ref('stg_lead') }}
)
select
    id,
    name,
    email,
    phone,
    stage_name,
    active,
    salesperson_id,
    customer_id,
    expected_revenue,
    priority,
    create_date,
    write_date,
    is_won
from source_data