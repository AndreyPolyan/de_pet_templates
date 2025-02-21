{{
    config(
        materialized='incremental',
        unique_key=['date_update', 'currency_from']
    )
}}

WITH operations AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

currencies AS (
    SELECT * FROM {{ ref('stg_currencies') }} 
),

increment AS (
    SELECT 
        o.*,
        c.currency_with_div,
        (o.amount * c.currency_with_div) AS amount_usd
    FROM operations o
    LEFT JOIN currencies c 
        ON o.currency_code = c.currency_code
        AND c.currency_code_with = 420
    WHERE o.transaction_dt::DATE = c.date_update::DATE
    AND o.status = 'done' -- Closed transactions only
)

SELECT 
    transaction_dt::date AS date_update,
    currency_code AS currency_from,
    SUM(amount_usd) AS amount_total,
    COUNT(amount_usd) AS cnt_transactions,
    ROUND(COUNT(operation_id) / COUNT(DISTINCT account_number_from), 3) AS avg_transactions_per_account,
    COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
FROM increment
GROUP BY 1, 2