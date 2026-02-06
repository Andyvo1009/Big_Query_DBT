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
            JSON_VALUE(data, '$.name') AS name,
            JSON_VALUE(data, '$.email') AS email,
            JSON_VALUE(data, '$.phone') AS phone,

            JSON_VALUE(data, '$.is_company') = 'true' AS is_company,

            -- parent_id: integer
            CAST(JSON_VALUE(data, '$.parent_id') AS INT64) AS parent_id,

            -- city: boolean in your JSON, not string
            JSON_VALUE(data, '$.city') = 'true' AS city,

            -- country_id is almost always null → safe cast
            CAST(JSON_VALUE(data, '$.country_id') AS INT64) AS country_id,

            -- ISO datetime → TIMESTAMP
            CAST(JSON_VALUE(data, '$.create_date') AS TIMESTAMP) AS create_date,
            event_timestamp
        FROM {{ source('bronze_layer', 'raw_customer') }}
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
    {{strip_html('name')}} AS name,
    LOWER(email) AS email,
    phone,
    is_company,
    parent_id,
    city,
    country_id,
    create_date
FROM ranked
WHERE rn = 1
