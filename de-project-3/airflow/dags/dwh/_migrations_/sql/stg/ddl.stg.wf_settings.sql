CREATE TABLE if not exists STG.wf_settings (
    id IDENTITY(1,1),
    workflow_key varchar(256), 
    workflow_settings LONG VARCHAR(2000)
)
ORDER BY ID
UNSEGMENTED ALL NODES;