import os
from logging import Logger
from pathlib import Path
from typing import Dict

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting


class DailyDatamarts:
    WF_KEY = "daily_reports_workflow"
    LAST_LAUNCH_ID = "last_launch_id"
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
        self.settings_repository = EtlSettingsRepository(schema='cdm')

    def excecute_scripts(self, path_to_scripts: str, params:Dict = {}) -> None:
        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} file(s) to apply changes.")


        for i, fp in enumerate(file_paths):
            self.log.info(f"Iteration {i+1}. Applying file {fp.name}")
            script = fp.read_text()
            self.log.info(f'Executing with params: {params}')
            with self._db.connection() as conn:
                wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
                if not wf_setting:
                    wf_setting = EtlSetting(
                                    id=0,
                                    workflow_key=self.WF_KEY,
                                    workflow_settings= {self.LAST_LAUNCH_ID: {}}
                                        )
                wf_setting_report = wf_setting.workflow_settings[self.LAST_LAUNCH_ID].get(fp.name)
                if not wf_setting_report:
                    wf_setting.workflow_settings[self.LAST_LAUNCH_ID][fp.name] = {'launch_id' : 0, 'params': params}
                last_launch_id = wf_setting.workflow_settings[self.LAST_LAUNCH_ID][fp.name]['launch_id']
                with conn.cursor() as cur:
                    params_exec = params.copy()
                    params_exec['launch_id'] = last_launch_id + 1
                    cur.execute(script, params_exec)
                    wf_setting.workflow_settings[self.LAST_LAUNCH_ID][fp.name]['launch_id'] = last_launch_id + 1
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
                    self.log.info(f"Iteration {i+1}. File {fp.name} executed successfully. WF params updated -> {wf_setting.workflow_settings[self.LAST_LAUNCH_ID][fp.name]}")
            
