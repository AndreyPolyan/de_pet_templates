from datetime import datetime
import json
from logging import Logger
from typing import List
from psycopg import Connection

from lib.dict_util import json2str
from lib import PgConnect
from lib.wf_settings import EtlSettingsRepository, EtlSetting

from analytics.dds.objects import BonusProductDdsObj, FactBonusSaleJsonObj, FactSaleDdsObj, OrderJsonObj

from analytics.dds.repositories import BonusRawRepository,OrderRawRepository,\
                                         FactSaleDdsRepository, ProductDdsRepository, OrderDdsRepository



class FctSaleLoader:
    WF_KEY = "sales_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 300
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw_order = OrderRawRepository()
        self.raw_bonus = BonusRawRepository()
        self.dds = FactSaleDdsRepository()
        self.dds_orders = OrderDdsRepository()
        self.dds_products = ProductDdsRepository()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log
    def parse_bonus_products(self, bonus_raw: FactBonusSaleJsonObj) -> List[BonusProductDdsObj]:
        res = []
        bonus_json = json.loads(bonus_raw.event_value)
        for prod_json in bonus_json['product_payments']:
            t = BonusProductDdsObj(
                              product_id=prod_json['product_id'],
                              bonus_payment=prod_json['bonus_payment'],
                              bonus_grant=prod_json['bonus_grant']
                              )
            res.append(t)
        return res
    def parse_order_products(self, conn: Connection, order_raw: OrderJsonObj, order_version_id: int) -> List[FactSaleDdsObj]:
        res = []
        order_json = json.loads(order_raw.object_value)
        order_ts = datetime.strptime(order_json["date"], "%Y-%m-%d %H:%M:%S")
        for prod_json in order_json['order_items']:
            product_id = prod_json['id']
            self.log.info(f'Trying to find product: product_id {product_id}, ts {order_ts}, order {order_version_id}')
            product_version = self.dds_products.get_product_version(conn, product_id, order_ts)
            if not product_version:
                self.log.error(f"Product {product_id} not found in DWH. Aborting...")
                raise KeyError('Product not found in DWH.')
            else : 
                    t = FactSaleDdsObj(id=0,
                              product_id=product_version.id,
                              product_key=product_version.product_id,
                              order_id = order_version_id,
                              count = int(prod_json['quantity']),
                              price= float(prod_json['price']),
                              total_sum= int(prod_json['quantity']) * float(prod_json['price']),
                              bonus_payment=0,
                              bonus_grant=0
                              )
            res.append(t)
        return res
    def load_data(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, 
                                        workflow_key=self.WF_KEY, 
                                        workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.raw_order.load_raw_orders_by_id(conn, last_loaded, self.BATCH_LIMIT)
            load_queue = [obj for obj in load_queue if json.loads(obj.object_value)['final_status'] == 'CLOSED']
            if not load_queue:
                self.log.info('No data to load. Aborting...')
                return
            products = self.dds_products.list_all_products(conn)
            prod_dict = {}
            for prod in products:
                prod_dict[prod.product_id] = prod

            
            load_queue.sort(key=lambda x: x.id)
            for order in load_queue:
                order_version = self.dds_orders.get_order(conn, order.object_id)
                if not order_version:
                    return
                bonus_obj = self.raw_bonus.get_event_by_order(conn, order_version.order_id)
                parsed_product_sales = self.parse_order_products(conn, order, order_version.id)
                for product in parsed_product_sales:
                    if product.id == -1:
                        return
                if not bonus_obj:
                    self.dds.insert_fctsales(conn,parsed_product_sales)
                else:
                    parsed_bonus_products = self.parse_bonus_products(bonus_obj)
                    parsed_bonus_products = [p for p in parsed_bonus_products if p.product_id in prod_dict]
                    bon_prod_dict = {}
                    for prod in parsed_bonus_products:
                        bon_prod_dict[prod.product_id] = prod
                    product_sales_to_load = []
                    for prod in parsed_product_sales:
                        bonus_product_sale = bon_prod_dict.get(prod.product_key)
                        if bonus_product_sale:
                            prod.bonus_grant = bonus_product_sale.bonus_grant
                            prod.bonus_payment = bonus_product_sale.bonus_payment
                            product_sales_to_load.append(prod)
                        else:
                            product_sales_to_load.append(prod)
                    self.dds.insert_fctsales(conn,product_sales_to_load)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)