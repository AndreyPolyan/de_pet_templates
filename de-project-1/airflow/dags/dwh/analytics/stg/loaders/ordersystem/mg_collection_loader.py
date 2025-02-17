from datetime import datetime
from logging import Logger

from lib.wf_settings import EtlSetting, EtlSettingsRepository
from analytics.stg.loaders.ordersystem.mg_collection_reader import MgCollectionReader
from lib import PgConnect, PgSaver
from lib.dict_util import json2str


class MgCollectionLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_reader: MgCollectionReader,
                pg_dest: PgConnect,
                pg_saver: PgSaver,
                logger: Logger,
                collection_name:str,
                WF_KEY:str = None) -> None:
        
        if not WF_KEY:
            raise NameError('WF Key setting name is not set')
        
        self.collection_reader = collection_reader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = logger
        self.collection_name = collection_name
        self.WF_KEY = WF_KEY

    def load_data(self) -> int:

        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2020, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_reader.get_collection(last_loaded_ts, self._SESSION_LIMIT, self.collection_name)
            self.log.info(f"Found {len(load_queue)} documents to sync from {self.collection_name} collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn=conn, id=str(d["_id"]), update_ts=d["update_ts"], val=d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing {self.collection_name} collection.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
