from datetime import datetime
import json
from logging import Logger
from typing import Dict, List
from psycopg import Connection

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting

from analytics.dds.objects import ProductDdsObj, RestaurantJsonObj
from analytics.dds.repositories import ProductDdsRepository, RestaurantDdsRepository, RestaurantRawRepository, TsDdsRepository



class ProductLoader:
    WF_KEY = "products_raw_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_ts = TsDdsRepository()
        self.dds_restaurants = RestaurantDdsRepository()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def parse_restaurants_menu(self, conn: Connection, restaurant_raw: RestaurantJsonObj, restaurant_version_id: int) -> List[ProductDdsObj]:
        res = []
        rest_json = json.loads(restaurant_raw.object_value)
        for prod_json in rest_json['menu']:
            exst_product = self.dds_products.get_product(conn, prod_json['_id'])
            active_from = datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S") if exst_product else datetime(1999, 1, 1)
            t = ProductDdsObj(id=0,
                              product_id=prod_json['_id'],
                              product_name=prod_json['name'],
                              product_price=prod_json['price'],
                              active_from=active_from,
                              active_to=datetime(year=2099, month=12, day=31),
                              restaurant_id=restaurant_version_id
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

            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_ts)
            load_queue.sort(key=lambda x: x.id)

            for restaurant in load_queue:
                restaurant_version = self.dds_restaurants.get_restaurant(conn, restaurant.object_id)
                rest_json = json.loads(restaurant.object_value)
                restaurant_update_ts = datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S")
                if not restaurant_version:
                    return
                products = self.dds_products.list_actual_products(conn,restaurant_version)

                curr_prod_dict = {}
                for p in products:
                    curr_prod_dict[p.product_id] = p

                products_to_load = self.parse_restaurants_menu(conn, restaurant, restaurant_version.id)
                menu_prod_dict = {}
                for p in products_to_load:
                    menu_prod_dict[p.product_id] = p
                
                new_products_to_load = [p for p in products_to_load if p.product_id not in curr_prod_dict]
                updated_products_to_load = [p for p in products_to_load if p.product_id in curr_prod_dict]
                old_products_to_load = [p for p in products if p.product_id not in menu_prod_dict]
                
                for p in new_products_to_load:
                    try:
                        self.log.info(f"Inserting new product: {p.product_id}, {p.product_name}")
                        self.dds_products.insert_dds_product(conn, p)
                    except Exception as e:
                        self.log.error(f"Failed to insert: {p.product_id}, {p.product_name}")
                        self.log.error(f"{e}")
    
                for p in updated_products_to_load:
                    if p != self.dds_products.get_product(conn, p.product_id):
                        self.log.info(f"Updating product: {p.product_id}, {p.product_name}")
                        try:
                            self.dds_products.update_product(conn, self.dds_products.get_product(conn, p.product_id), restaurant_update_ts)
                            self.dds_products.insert_dds_product(conn, p)
                        except Exception as e:
                            self.log.error(f"Failed to update: {p.product_id}, {p.product_name}")
                            self.log.error(f"{e}")

                for p in old_products_to_load:
                    self.log.info(f"Deactivating product: {p.product_id}, {p.product_name}")
                    try:
                        self.dds_products.update_product(conn, p, restaurant_update_ts)
                    except Exception as e:
                            self.log.error(f"Failed to deactivate: {p.product_id}, {p.product_name}")
                            self.log.error(f"{e}")    
            if load_queue:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([json.loads(t.object_value)["update_ts"] for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)