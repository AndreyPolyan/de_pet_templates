CREATE TABLE if not exists STG.currencies (
    id IDENTITY(1,1), 
    date_update timestamp(0) NULL,
    currency_code smallint NULL,
    currency_code_with smallint NULL,
    currency_with_div numeric(5, 3) NULL
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION STG.currencies /*+createtype(P)*/ 
(
    id,
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
AS
 SELECT 
        currencies.id,
        currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_with_div
 FROM 
    STG.currencies
 ORDER BY currencies.id
SEGMENTED BY hash(currencies.id) ALL NODES KSAFE 1;