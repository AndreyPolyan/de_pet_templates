drop schema if exists online_sales CASCADE;

drop schema if exists store CASCADE;


DROP TABLE IF EXISTS public.inventory_fact;
DROP TABLE IF EXISTS public.customer_dimension;
DROP TABLE IF EXISTS public.product_dimension;
DROP TABLE IF EXISTS public.promotion_dimension;
DROP TABLE IF EXISTS public.date_dimension;
DROP TABLE IF EXISTS public.vendor_dimension;
DROP TABLE IF EXISTS public.employee_dimension;
DROP TABLE IF EXISTS public.shipping_dimension;
DROP TABLE IF EXISTS public.warehouse_dimension;
DROP TABLE IF EXISTS public.vmart_load_success;

CREATE SCHEMA IF NOT EXISTS STG;
CREATE SCHEMA IF NOT EXISTS CDM;


CREATE USER de with password 'engineer';

grant all privileges on schema CDM to de;

grant all privileges on schema STG to de;

grant all privileges on schema public to de;
ALTER USER dbadmin IDENTIFIED BY 'secretpassword';