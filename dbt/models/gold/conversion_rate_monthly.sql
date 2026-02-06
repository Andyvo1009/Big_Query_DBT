{{ config(
    materialized='view',
    schema='gold_layer'
) }}

WITH leads AS (
    SELECT
        DATE_TRUNC(create_date, MONTH) AS month,
        COUNT(*) AS total_leads,
        COUNTIF(is_won = TRUE) AS won_leads
    FROM {{ ref('stg_lead') }}
    GROUP BY 1
)

SELECT
    month,
    total_leads,
    won_leads,
    SAFE_DIVIDE(won_leads, total_leads) AS conversion_rate
FROM leads
ORDER BY month
