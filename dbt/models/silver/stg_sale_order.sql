{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='silver_layer'
) }}

WITH source_data AS (

    SELECT
    -- Keys & IDs: Extracted using JSON_VALUE and cast to INT64 (BigQuery standard for large integers/IDs)
            CAST(JSON_VALUE(data, '$.order_line_id') AS INT64) AS id,
            CAST(JSON_VALUE(data, '$.order_id') AS INT64) AS order_id,
            CAST(JSON_VALUE(data, '$.customer_id') AS INT64) AS customer_id,
            CAST(JSON_VALUE(data, '$.salesperson_id') AS INT64) AS salesperson_id,
            CAST(JSON_VALUE(data, '$.product_id') AS INT64) AS product_id,

            -- Date: Extracted and cast to DATE
            -- JSON_VALUE extracts the string, which is then parsed by the CAST function
            CAST(JSON_VALUE(data, '$.order_date') AS TIMESTAMP) AS order_date,

            -- Status: Extracted as standard strings. JSON_VALUE returns NULL if the field doesn't exist.
            JSON_VALUE(data, '$.state') AS order_state,
            JSON_VALUE(data, '$.invoice_status') AS invoice_status,

            -- Line Amounts: Extracted as BIGNUMERIC for precise calculations of currency and quantities
            CAST(JSON_VALUE(data, '$.quantity') AS INT64) AS quantity,
            CAST(JSON_VALUE(data, '$.price_unit') AS FLOAT64) AS price_unit,
            CAST(JSON_VALUE(data, '$.discount_pct') AS FLOAT64) AS discount_pct,
            CAST(JSON_VALUE(data, '$.subtotal_amount') AS FLOAT64) AS subtotal_amount,
            CAST(JSON_VALUE(data, '$.total_amount') AS FLOAT64) AS total_amount,

            -- Order-level Payment Info (also cast as BIGNUMERIC)
            CAST(JSON_VALUE(data, '$.order_total_amount') AS FLOAT64) AS order_total_amount,
            CAST(JSON_VALUE(data, '$.order_total_invoiced') AS FLOAT64) AS order_total_invoiced,
            CAST(JSON_VALUE(data, '$.order_residual_amount') AS FLOAT64) AS order_residual_amount,
            JSON_VALUE(data, '$.order_payment_state') AS order_payment_state,


            event_timestamp
    FROM {{ source('bronze_layer', 'raw_saleorder') }}
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
    order_id,
    customer_id,
    salesperson_id,
    product_id,
    order_date,
    order_state,
    invoice_status,
    quantity,
    price_unit,
    discount_pct,
    subtotal_amount,
    total_amount,
    order_total_amount,
    order_total_invoiced,
    order_residual_amount,
    order_payment_state

FROM ranked
WHERE rn = 1
