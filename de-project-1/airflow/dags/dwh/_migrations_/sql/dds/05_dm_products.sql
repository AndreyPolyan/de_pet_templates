CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial4 NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) DEFAULT 0 NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
    CONSTRAINT dm_products_product_id_unique UNIQUE (product_id),
	CONSTRAINT dm_products_product_price_check CHECK ((product_price >= (0)::numeric))
);
