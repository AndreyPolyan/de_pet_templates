from datetime import datetime, timezone
from logging import Logger
import json

import pandas as pd

from lib.kafka_connect import KafkaConsumer
from loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size:int,
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")
        processed_messages = 0
        timeout: float = 3.0

        while processed_messages < self._batch_size:
            dct_msg = self._kafka_consumer.consume(timeout=timeout)
            if not dct_msg:
                break
            self._logger.info(f"{datetime.now(timezone.utc)}: New message received")
            try:
                frmt_dct_msg = json.loads(dct_msg)
            except:
                self._logger.info(f"{datetime.now(timezone.utc)}: Broken message:{dct_msg}\nSkipping..")
                continue
            if len(frmt_dct_msg) == 0: 
                continue
            
            try:
                data = pd.json_normalize(frmt_dct_msg)\
                        .query('object_type == "cdm_order_event"')
            except: 
                self._logger.info(f"{datetime.now(timezone.utc)}: Broken message:{dct_msg}\nSkipping..")
                continue

            if data.empty:
                continue

            usr_cat_cnt = data[['payload.user_id','payload.category_id'
                                ,'payload.category_name','payload.order_id']]\
                            .groupby(['payload.user_id'
                                      ,'payload.category_id'
                                      ,'payload.category_name'],as_index=False)\
                            .agg({'payload.order_id':'nunique'})\
                            .rename(columns={'payload.user_id':'user_id'
                                             ,'payload.category_id':'category_id'
                                             ,'payload.category_name':'category_name'
                                             ,'payload.order_id':'order_cnt'})
            
            usr_prd_cnt = data[['payload.user_id','payload.product_id'
                                ,'payload.product_name','payload.order_id']]\
                            .groupby(['payload.user_id'
                                      ,'payload.product_id'
                                      ,'payload.product_name'],as_index=False)\
                            .agg({'payload.order_id':'count'})\
                            .rename(columns={'payload.user_id':'user_id'
                                             ,'payload.product_id':'product_id'
                                             ,'payload.product_name':'product_name'
                                             ,'payload.order_id':'order_cnt'})
            if not usr_cat_cnt.empty:
                usr_cat_cnt\
                    .apply(lambda x: 
                                self._cdm_repository.upsert_user_category_counters
                                (
                                x['user_id']
                                ,x['category_id']
                                ,x['category_name']
                                ,x['order_cnt']
                                )
                            ,axis= 1)
            if not usr_prd_cnt.empty:
                usr_prd_cnt\
                    .apply(lambda x: 
                                self._cdm_repository.user_product_counters_upsert
                                (
                                x['user_id']
                                ,x['product_id']
                                ,x['product_name']
                                ,x['order_cnt']
                                )
                            ,axis= 1)

            #Increment over messages for batch limit
            processed_messages += 1

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")