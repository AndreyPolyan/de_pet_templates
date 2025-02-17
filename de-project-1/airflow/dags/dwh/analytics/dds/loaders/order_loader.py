from datetime import datetime
import json
from logging import Logger

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting
from analytics.dds.repositories import OrderDdsRepository, OrderRawRepository, RestaurantDdsRepository, TsDdsRepository, TsDdsRepository, UserDdsRepository
from analytics.dds.objects import OrderDdsObj, OrderJsonObj, TsDdsObj

class OrderLoader:
    WF_KEY = "orders_raw_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 1000

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = OrderRawRepository()
        self.dds = OrderDdsRepository()
        self.dds_restaurants = RestaurantDdsRepository()
        self.dds_users = UserDdsRepository()
        self.dds_ts = TsDdsRepository()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def parse_order(self, order: OrderJsonObj, restaurant_version_id: int, timestamp_version_id: int, user_version_id: int) -> OrderDdsObj:
        order_json = json.loads(order.object_value)
        t = OrderDdsObj(id=order.id,
                                 order_id= order_json['_id'],
                                 order_status= order_json['final_status'],
                                 restaurant_id= restaurant_version_id,
                                 timestamp_id= timestamp_version_id,
                                 user_id = user_version_id
                                 )
        return t

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

            load_queue = self.raw.load_raw_orders(conn, last_loaded_ts, self.BATCH_LIMIT)
            load_queue.sort(key=lambda x: x.id)
            self.log.info('Starting parsing orders...')
            for order in load_queue:
                order_json = json.loads(order.object_value)
                restaurant_id = order_json['restaurant']['id']
                ts_id = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
                user_id = order_json['user']['id']

                restaurant_version = self.dds_restaurants.get_restaurant(conn, restaurant_id)
                ts_version = self.dds_ts.get_ts(conn, ts_id)
                user_version = self.dds_users.get_user(conn, user_id)
                if (not restaurant_version): 
                    self.log.error(f"Restaurant not found in DWH {restaurant_id}. Skipping...")
                    return
                elif (not ts_version): 
                    self.log.error(f"Timestamp not found in DWH {ts_id}. Adding to DM...")
                    t = TsDdsObj(id=0,
                     ts=ts_id,
                     year=ts_id.year,
                     month=ts_id.month,
                     day=ts_id.day,
                     time=ts_id.time(),
                     date=ts_id.date()
                    )
                    self.dds_ts.insert_dds_ts(conn, t)
                    ts_version = self.dds_ts.get_ts(conn, ts_id)
                elif (not user_version):
                    self.log.error(f"User not found in DWH {user_id}. Skipping...")
                    return
                parsed_order = self.parse_order(order, restaurant_version.id, ts_version.id, user_version.id)
                existing = self.dds.get_order(conn, parsed_order.order_id)
                if not existing:
                    self.dds.insert_order(conn, parsed_order)
                elif existing != parsed_order:
                    self.dds.update_order(conn, parsed_order)
            if load_queue:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([json.loads(t.object_value)["update_ts"] for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)