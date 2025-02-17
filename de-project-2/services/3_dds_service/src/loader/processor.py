import json
import uuid
from datetime import datetime, timezone
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size:int, 
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._kafka_producer = kafka_producer
        self._dds_repository = dds_repository
        self._logger = logger
        # Setting hardcode batch size
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        processed_messages = 0
        timeout: float = 3.0
        load_src = 'service_stg'
        
        while processed_messages < self._batch_size:
            dct_msg = self._kafka_consumer.consume(timeout=timeout)

            #Stop if no messages in the queue
            if not dct_msg:
                break
            try:
                dct_msg = json.loads(dct_msg)
            except:
                self._logger.error(f"{datetime.now(timezone.utc)}: Couldn't convert data to dict")
                continue

            self._logger.info(f"{datetime.now(timezone.utc)}: New message received")
            self._logger.info(f"{datetime.now(timezone.utc)}: {dct_msg}")

            #Skip if broken message
            if 'object_type' not in dct_msg.keys():
                self._logger.info(f"No object type in message: {dct_msg}\nSkipping..")
                continue
            #Skip if not appropriate type of message
            if dct_msg.get('object_type') != 'order' or not dct_msg.get('payload'):
                continue

            # Better to use same load_dt for same message
            load_dt = datetime.now(timezone.utc)
            payload = dct_msg.get('payload')

            # Upserting ORDER
            self._logger.info(f"{datetime.now(timezone.utc)}: Processing ORDER from message")
            order_id = payload.get('id')
            h_order_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))
            order_dt = payload.get('date')
            order_cost = payload.get('cost')
            order_payment = payload.get('payment')
            order_status = payload.get('status')

            self._dds_repository.upsert_order(
                h_order_pk
                ,order_id
                ,order_dt
                ,load_dt
                ,load_src
                ,order_cost
                ,order_payment
                ,order_status
            )

            # Upserting USER
            self._logger.info(f"{datetime.now(timezone.utc)}: Processing USER from message")
            user_id = payload.get('user').get('id')
            h_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(user_id))
            username = payload.get('user').get('name')
            userlogin = username
            if 'login' in payload.get('user').keys():
                userlogin = payload.get('user').get('login')

            self._dds_repository.upsert_user(
                h_user_pk
                ,user_id
                ,load_dt
                ,load_src
                ,username
                ,userlogin
            )

            #Upserting LINK_ORDER_USER
            self._logger.info(f"{datetime.now(timezone.utc)}: Processing LINK_ORDER_USER from message")
            self._dds_repository.upsert_l_order_user(
                uuid.uuid3(uuid.NAMESPACE_X500, str(h_order_pk) + '_' + str(h_user_pk))
                ,h_order_pk
                ,h_user_pk
                ,load_dt
                ,load_src
            )

            #Upserting RESTAURANT
            self._logger.info(f"{datetime.now(timezone.utc)}: Processing RESTAURANT from message")
            restaurant_id = payload.get('restaurant').get('id')
            h_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(restaurant_id))
            restaurant_name = payload.get('restaurant').get('name')

            self._dds_repository.upsert_restaurant(
                h_restaurant_pk
                ,restaurant_id
                ,load_dt
                ,load_src
                ,restaurant_name
            )

            #Upserting PRODUCT / CATEGORY
            products = {}
            categories = {}

            for it in payload.get('products'):
                category = it.get('category')
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(category))
                categories[h_category_pk] = category
                product_id = it.get('id')
                h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(product_id))
                product_name = it.get('name')
                products[h_product_pk] = {'name': product_name
                                          ,'business_id': product_id
                                          ,'h_category_pk': h_category_pk
                                          ,'category_name': category
                                          , 'hk_product_category_pk': uuid.uuid3(uuid.NAMESPACE_X500, str(h_product_pk) + '_' + str(h_category_pk))}

            
            self._logger.info(f"{datetime.now(timezone.utc)}: Processing CATEGORY from message")
            for h_category_pk, category_name in categories.items():
                self._dds_repository.upsert_category(
                    h_category_pk
                    ,load_dt
                    ,load_src
                    ,category_name
                )
            
            #Products Loop
            cdm_msg = []
            self._logger.info(f"{datetime.now(timezone.utc)}: Processing PRODUCT and \nLINK_PRODUCT_CATEGORY/LINK_ORDER_PRODUCT/LINK_PRODUCT_RESTAURANT\nfrom message")
            for h_product_pk, product_item in products.items():
                self._dds_repository.upsert_product(
                    h_product_pk
                    ,product_item.get('business_id')
                    ,load_dt
                    ,load_src
                    ,product_item.get('name')
                )
                self._dds_repository.upsert_l_product_category(
                    product_item.get('hk_product_category_pk')
                    ,h_product_pk
                    ,product_item.get('h_category_pk')
                    ,load_dt
                    ,load_src
                )
                self._dds_repository.upsert_l_order_product(
                    uuid.uuid3(uuid.NAMESPACE_X500, str(h_order_pk) + '_' + str(h_product_pk))
                    ,h_order_pk
                    ,h_product_pk
                    ,load_dt
                    ,load_src
                )
                self._dds_repository.upsert_l_product_restaurant(
                    uuid.uuid3(uuid.NAMESPACE_X500, str(h_product_pk) + '_' + str(h_restaurant_pk))
                    ,h_product_pk
                    ,h_restaurant_pk
                    ,load_dt
                    ,load_src
                )
                if order_status == 'CLOSED':
                    cdm_msg.append(
                        {
                          "object_id": str(uuid.uuid4()),
                          "object_type": 'cdm_order_event',
                          'payload':{
                              'order_id': str(h_order_pk),
                              'user_id': str(h_user_pk),
                              'product_id': str(h_product_pk),
                              'product_name': str(product_item.get('name')),
                              'category_id': str(product_item.get('h_category_pk')),
                              'category_name': str(product_item.get('category_name'))
                            }  
                        }
                    )
            if cdm_msg:
                msg = json.dumps(cdm_msg,ensure_ascii=False)
                self._logger.info(f"{datetime.now(timezone.utc)} : SENDING CDM ORDER EVENT : {msg}")
                self._kafka_producer.produce(msg)
            #Increment over messages for batch limit
            processed_messages += 1

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
