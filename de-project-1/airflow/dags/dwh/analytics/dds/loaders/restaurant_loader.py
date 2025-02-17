from datetime import datetime
import json
from logging import Logger
from typing import  List

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting

from analytics.dds.repositories import RestaurantDdsRepository, RestaurantRawRepository
from analytics.dds.objects import RestaurantDdsObj, RestaurantJsonObj


class RestaurantLoader:
    WF_KEY = "restaurants_raw_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds = RestaurantDdsRepository()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.object_value)
            t = RestaurantDdsObj(id=r.id,
                                 restaurant_id=rest_json['_id'],
                                 restaurant_name=rest_json['name'],
                                 active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                 active_to=datetime(year=2099, month=12, day=31)
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
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]

            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_ts)
            load_queue.sort(key=lambda x: x.id)
            restaurants_to_load = self.parse_restaurants(load_queue)
            for r in restaurants_to_load:
                existing = self.dds.get_restaurant(conn, r.restaurant_id)
                if not existing:
                    self.log.info(f"Inserting: {r}")
                    try:
                        self.dds.insert_restaurant(conn, r)
                    except:
                        self.log.error(f"Failed to insert: {r}")
                elif existing != r :
                    self.log.info(f"Updating: {r}")
                    try:
                        self.dds.update_restaurant(conn, r)
                    except Exception as e:
                        self.log.error(f"Failed to update: {r}")
                        self.log.error(f"{e}")
            if load_queue:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([json.loads(t.object_value)["update_ts"] for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)