CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
    CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id),
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id)
);