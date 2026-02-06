{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='gold_layer'
) }}

with source_data as(
    select
        id,
        name,
        email,
        phone,
        is_company,
        parent_id,
        city,
        country_id,
        create_date
        from {{ ref('stg_customer') }}
)
select
    id,
    name,
    email,
    phone,
    is_company,
    parent_id,
    city,
    country_id,
    create_date
from source_data