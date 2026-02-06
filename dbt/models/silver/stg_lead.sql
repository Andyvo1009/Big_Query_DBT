{{ config(
    materialized="incremental",
    unique_key="id",
    incremental_strategy="merge",
    schema="silver_layer"
) }}

WITH source_data AS (
    SELECT
        cast(JSON_VALUE(data, '$.id') as INT64) AS id,
        JSON_VALUE(data, '$.name') AS name,
        JSON_VALUE(data, '$.email') AS email,
        JSON_VALUE(data, '$.phone') AS phone,
        CAST(JSON_VALUE(data, '$.stage_name') as STRING) AS stage_name,
        CAST(coalesce(JSON_VALUE(data, '$.active'), 'true') as BOOL) AS active,
        CAST(coalesce(JSON_VALUE(data, '$.country'), 'Việt Nam') as STRING) AS country,
        CAST(JSON_VALUE(data, '$.user_id') AS INT64) AS salesperson_id,
        CAST(JSON_VALUE(data, '$.customer_id') AS INT64) AS customer_id,
        CAST(JSON_VALUE(data, '$.expected_revenue') AS FLOAT64) AS expected_revenue,
        JSON_VALUE(data, '$.priority') AS priority,
        CAST(JSON_VALUE(data, '$.create_date') AS TIMESTAMP) AS create_date,
        CAST(JSON_VALUE(data, '$.write_date') AS TIMESTAMP) AS write_date,
        cast(JSON_VALUE(data, '$.is_won') as BOOL)  AS is_won,
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
    stage_name,
    coalesce(active,TRUE) AS active,
    country,
    salesperson_id ,
    customer_id,
    expected_revenue,
    priority,
    create_date,
    write_date,
    is_won
FROM ranked
WHERE rn = 1
