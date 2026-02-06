{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='gold_layer'
) }}

with source_data as(
    select id,
    author_id,
    email_from,
    reply_to,
    message_type,
    is_internal,
    subtype,
    body_length,
    partner_id,
    partner_name,
    date
    from {{ ref('stg_activity') }}
)
select
    id,
    author_id,
    email_from,
    reply_to,
    message_type,
    is_internal,
    subtype,
    body_length,
    partner_id,
    partner_name,
    date
from source_data
where is_internal=FALSE