{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='silver_layer'
) }}
WITH
    source_data
    AS
    (
        SELECT
            CAST(JSON_VALUE(data, '$.id') AS INT64) AS id,
            JSON_VALUE(data, '$.author_id') AS author_id,
            JSON_VALUE(data, '$.email_from') AS email_from,
            JSON_VALUE(data, '$.reply_to') AS reply_to,

            JSON_VALUE(data, '$.message_type') AS message_type,

            -- parent_id: integer
            CAST(JSON_VALUE(data, '$.is_internal') AS BOOL) AS is_internal,

            -- city: boolean in your JSON, not string
            JSON_VALUE(data, '$.body_length')  AS body_length,
            JSON_VALUE(data, '$.subtype')  AS subtype,
            JSON_VALUE(data, '$.partner_id')  AS partner_id,
            JSON_VALUE(data, '$.partner_name')  AS partner_name,
            CAST(JSON_VALUE(data, '$.date') AS TIMESTAMP) AS date,
           
            event_timestamp
        FROM {{ source('bronze_layer', 'raw_activity') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM source_data
)

SELECT
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
FROM ranked
WHERE rn = 1
