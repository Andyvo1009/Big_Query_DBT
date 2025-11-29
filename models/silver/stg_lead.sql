{{ config(
    materialized="incremental",
    unique_key="id",
    incremental_strategy="merge"
) }}

WITH source_data AS (
    SELECT
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.name') AS name,
        JSON_VALUE(data, '$.email') AS email,
        JSON_VALUE(data, '$.phone') AS phone,
        CAST(JSON_VALUE(data, '$.stage_id') AS INT64) AS stage_id,
        CAST(JSON_VALUE(data, '$.user_id') AS INT64) AS user_id,
        CAST(JSON_VALUE(data, '$.expected_revenue') AS FLOAT64) AS expected_revenue,
        JSON_VALUE(data, '$.priority') AS priority,
        CAST(JSON_VALUE(data, '$.create_date') AS TIMESTAMP) AS create_date,
        CAST(JSON_VALUE(data, '$.write_date') AS TIMESTAMP) AS write_date,
        JSON_VALUE(data, '$.is_won') = 'true' AS is_won,
        event_timestamp
    FROM {{ source('bronze_layer', 'raw_lead') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id ORDER BY event_timestamp DESC
        ) AS rn
    FROM source_data
)

SELECT
    id,
    name,
    LOWER(email) AS email,
    phone,
    stage_id,
    user_id,
    expected_revenue,
    priority,
    create_date,
    write_date,
    is_won
FROM ranked
WHERE rn = 1
