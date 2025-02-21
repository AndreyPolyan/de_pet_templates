{{
    config(
        materialized='ephemeral'
    )
}}

WITH enumerated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY operation_id, transaction_dt) AS rn
    FROM {{ source('STG', 'transactions') }}
    WHERE transaction_dt::date = '{{ var("launch_date") }}'::date
)

SELECT 
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM enumerated
WHERE TRUE
            AND rn = 1 
            AND account_number_from > 0 -- No Test Accounts
            AND operation_id IS NOT NULL 
            AND account_number_from IS NOT NULL
            AND account_number_to IS NOT NULL
            AND currency_code IS NOT NULL
            AND country IS NOT NULL
            AND status IS NOT NULL
            AND transaction_type IS NOT NULL
            AND amount IS NOT NULL
            AND transaction_dt IS NOT NULL