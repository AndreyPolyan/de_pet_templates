from datetime import datetime
import json
from logging import Logger

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting

from analytics.dds.repositories import OrderRawRepository, TsDdsRepository
from analytics.dds.objects import OrderJsonObj, TsDdsObj



class TimeLoader:
    WF_KEY = "time_raw_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = OrderRawRepository()
        self.dds = TsDdsRepository()
        self.settings_repository = EtlSettingsRepository(schema = 'dds')
        self.log = log
    def parse_order_timestamp(self, order_raw: OrderJsonObj) -> TsDdsObj:
        rest_json = json.loads(order_raw.object_value)
        dt = datetime.strptime(rest_json['date'], "%Y-%m-%d %H:%M:%S")
        t = TsDdsObj(id=0,
                     ts=dt,
                     year=dt.year,
                     month=dt.month,
                     day=dt.day,
                     time=dt.time(),
                     date=dt.date()
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
            load_queue = self.raw.load_raw_orders(conn, last_loaded_ts)
            # load_queue = [order for order in load_queue if json.loads(order.object_value)['final_status'] in ('CLOSED','CANCELLED')]
            tss = self.dds.list_ts(conn)
            ts_dict = {}
            for t in tss:
                ts_dict[t.ts] = t
            for order in load_queue:
                parsed_ts = self.parse_order_timestamp(order)
                if parsed_ts.ts not in ts_dict:
                    self.log.info(f"Inserting new TS: {parsed_ts.ts}")
                    try:
                        self.dds.insert_dds_ts(conn, parsed_ts)
                        ts_dict[parsed_ts.ts] = parsed_ts
                    except Exception as e:
                        self.log.error(f"Failed to load {parsed_ts.ts}")
                        self.log.error(f"{e}")
            if load_queue:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([json.loads(t.object_value)["update_ts"] for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)