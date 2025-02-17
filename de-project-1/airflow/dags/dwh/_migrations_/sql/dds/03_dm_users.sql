CREATE TABLE IF NOT EXISTS dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id),
	CONSTRAINT dm_users_user_id_uindex UNIQUE (user_id)
);