{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

with
    source_data as (
        select
            json_value(data, '$.id') as id,
            json_value(data, '$.name') as name,
            json_value(data, '$.login') as login,
            json_value(data, '$.email') as email,
            cast(json_value(data, '$.active') as bool) as active,
            json_value(data, '$.team_name') as team_name,
            cast(json_value(data, '$.create_date') as timestamp) as create_date,
            event_timestamp
        from {{ source('bronze_layer', 'raw_salesperson') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by id
            order by event_timestamp desc
        ) as rn
    from source_data
)

select
    id,
    {{strip_html('name')}} AS name,
    login,
    lower(email) as email,
    active,
    team_name,
    create_date
from ranked
where rn = 1
