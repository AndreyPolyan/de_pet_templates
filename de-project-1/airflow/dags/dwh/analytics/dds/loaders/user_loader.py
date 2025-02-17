from datetime import datetime
import json
from logging import Logger
from typing import List

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting

from analytics.dds.repositories import UserDdsRepository, UserRawRepository
from analytics.dds.objects import UserDdsObj, UserJsonObj

class UserLoader:
    WF_KEY = "users_raw_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = UserRawRepository()
        self.dds = UserDdsRepository()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def parse_users(self, raws: List[UserJsonObj]) -> List[UserDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.object_value)
            t = UserDdsObj(id=r.id,
                                 user_id=rest_json['_id'],
                                 user_name=rest_json['name'],
                                 user_login=rest_json['login']
                                 )
            res.append(t)
        return res

    def load_data(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2020, 1, 1).isoformat()
                    }
                )

            last_loaded_ts = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]

            load_queue = self.raw.load_raw_users(conn, last_loaded_ts)
            load_queue.sort(key=lambda x: x.id)
            users_to_load = self.parse_users(load_queue)
            for r in users_to_load:
                existing = self.dds.get_user(conn, r.user_id)
                self.log.info(f"Existing: {existing}")
                if not existing:
                    self.log.info(f"Inserting: {r}")
                    try:
                        self.dds.insert_user(conn, r)
                    except:
                        self.log.error(f"Failed to insert: {r}")
                elif existing != r :
                    self.log.info(f"Updating: {r}")
                    try:
                        self.dds.update_user(conn, r)
                    except Exception as e:
                        self.log.error(f"Failed to update: {r}")
                        self.log.error(f"{e}")
            if load_queue:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([json.loads(t.object_value)["update_ts"] for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)