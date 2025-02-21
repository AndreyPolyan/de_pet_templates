{{
    config(
        materialized='ephemeral'
    )
}}

WITH enumerated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY currency_code, currency_code_with, date_update) AS rn
    FROM {{ source('STG', 'currencies')}}
    WHERE date_update::date = '{{ var("launch_date") }}'::date
)

SELECT 
    id,
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
FROM enumerated
WHERE rn = 1
AND id IS NOT NULL
AND date_update IS NOT NULL
AND currency_code IS NOT NULL
AND currency_code_with IS NOT NULL
AND currency_with_div IS NOT NULL