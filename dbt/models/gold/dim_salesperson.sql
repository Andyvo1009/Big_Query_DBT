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
        active,
        team_name,
        create_date
    from {{ ref('stg_salesperson') }}
)
select
    id,
    name,
    email,
    phone,
    active,
    team_name,
    create_date
from source_data