CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int4 NOT NULL,
	"month" int4 NOT NULL,
	"day" int4 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
    CONSTRAINT dm_timestamps_ts_unique UNIQUE (ts),
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);