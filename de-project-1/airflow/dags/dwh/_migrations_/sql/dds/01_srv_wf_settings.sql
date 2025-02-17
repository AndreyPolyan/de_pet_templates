CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
	id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);