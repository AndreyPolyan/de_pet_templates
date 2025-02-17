CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	order_status varchar NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	user_id int4 NOT NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
    CONSTRAINT dm_orders_order_id_unique UNIQUE (order_id),
    CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
    CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);