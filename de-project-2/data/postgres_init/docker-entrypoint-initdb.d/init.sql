CREATE DATABASE dwh;

CREATE USER de WITH PASSWORD 'engineer';
grant all privileges on database "dwh" to de;
ALTER USER de CREATEDB;

\connect dwh;

set role de;

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;


-- STG LAYER
create table if not exists stg.order_events (
    id SERIAL PRIMARY KEY,
    object_id integer unique not null,
    object_type varchar(32) not null,
    sent_dttm timestamp not null,
    payload json not null  
);

-- DDS LAYER

create table if not exists dds.h_user (
	h_user_pk uuid unique not null,
	user_id varchar not null,
	load_dt timestamp not null,
	load_src varchar (32) default 'orders-system-kafka' not null
);

create table if not exists dds.h_product (
	h_product_pk uuid unique not null,
	product_id varchar not null,
	load_dt timestamp not null,
	load_src varchar (32) default 'orders-system-kafka' not null
);

create table if not exists dds.h_category (
	h_category_pk uuid unique not null,
	category_name varchar not null,
	load_dt timestamp not null,
	load_src varchar (32) default 'orders-system-kafka' not null
);

create table if not exists dds.h_restaurant (
	h_restaurant_pk uuid unique not null,
	restaurant_id varchar not null,
	load_dt timestamp not null,
	load_src varchar (32) default 'orders-system-kafka' not null
);

create table if not exists dds.h_order (
	h_order_pk uuid unique not null,
	order_id integer not null,
	order_dt timestamp not null,
	load_dt timestamp not null,
	load_src varchar (32) default 'orders-system-kafka' not null
);


create table if not exists dds.l_order_product (
	hk_order_product_pk uuid unique not null,
	h_order_pk uuid not null,
	h_product_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	CONSTRAINT fk_order FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
	CONSTRAINT fk_product FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk)
);

create table if not exists dds.l_product_restaurant (
	hk_product_restaurant_pk uuid unique not null,
	h_product_pk uuid not null,
	h_restaurant_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	CONSTRAINT fk_product FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
	CONSTRAINT fk_restaurant FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk)
);

create table if not exists dds.l_product_category (
	hk_product_category_pk uuid unique not null,
	h_product_pk uuid not null,
	h_category_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	CONSTRAINT fk_product FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk),
	CONSTRAINT fk_category FOREIGN KEY (h_category_pk) REFERENCES dds.h_category (h_category_pk)
);

create table if not exists dds.l_order_user (
	hk_order_user_pk uuid unique not null,
	h_order_pk uuid not null,
	h_user_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	CONSTRAINT fk_order FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk),
	CONSTRAINT fk_user FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk)
);

create table if not exists dds.s_user_names (
	h_user_pk uuid unique not null,
	username varchar (255) not null,
	userlogin varchar (255) not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	hk_user_names_hashdiff uuid unique not null,
	PRIMARY KEY(h_user_pk, load_dt),
	CONSTRAINT fk_user_sat FOREIGN KEY (h_user_pk) REFERENCES dds.h_user (h_user_pk)
);

create table if not exists dds.s_product_names (
	h_product_pk uuid unique not null,
	name varchar (255) not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	hk_product_names_hashdiff uuid unique not null,
	PRIMARY KEY(h_product_pk, load_dt),
	CONSTRAINT fk_product_sat FOREIGN KEY (h_product_pk) REFERENCES dds.h_product (h_product_pk)
);

create table if not exists dds.s_restaurant_names (
	h_restaurant_pk uuid unique not null,
	name varchar (255) not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	hk_restaurant_names_hashdiff uuid unique not null,
	PRIMARY KEY(h_restaurant_pk, load_dt),
	CONSTRAINT fk_restaurant_sat FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant (h_restaurant_pk)
);

create table if not exists dds.s_order_cost (
	h_order_pk uuid unique not null,
	cost decimal(19, 5) not null,
	payment decimal(19, 5) not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	hk_order_cost_hashdiff uuid unique not null,
	PRIMARY KEY(h_order_pk, load_dt),
	constraint cost_m check (cost >=0),
	constraint payment_m check (payment >=0),
	CONSTRAINT fk_order_cost_sat FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk)
);

create table if not exists dds.s_order_status (
	h_order_pk uuid unique not null,
	status varchar (255) not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null,
	hk_order_status_hashdiff uuid unique not null,
	PRIMARY KEY(h_order_pk, load_dt),
	CONSTRAINT fk_order_sat FOREIGN KEY (h_order_pk) REFERENCES dds.h_order (h_order_pk)
);

-- CDM LAYER
create table if not exists cdm.user_product_counters (
	id	serial primary key,
	user_id	uuid not null ,
	product_id	uuid not null,
	product_name	varchar not null,
	order_cnt int check (order_cnt >= 0) not null,
	constraint user_product_counters_unq unique(user_id,product_id)
);

create table if not exists cdm.user_category_counters (
	id	serial primary key,
	user_id	uuid not null,
	category_id	uuid not null,
	category_name	varchar not null,
	order_cnt int check (order_cnt >= 0) not null,
	constraint user_category_counters_unq unique(user_id,category_id)
);




