from typing import Dict, List
from datetime import datetime, timezone
from logging import Logger
import json

from loader.repository import StgRepository
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient


class StgMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size:int, 
                 logger: Logger) -> None:
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = batch_size
    
    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name, login) -> Dict[str, str]:
        return {
            "id": id,
            "name": name,
            "login": login
        }

    def _format_products(self, ordered_items, restaurant) -> List[Dict[str, str]]:
        order_products = []

        menu = restaurant.get("menu")
        for it in ordered_items:
            menu_item = next(x for x in menu if x.get('_id') == it.get('id'))
            dst_it = {
                "id": it.get("id"),
                "price": it.get("price") or menu_item.get('price'),
                "quantity": it.get("quantity"),
                "name": menu_item.get("name"),
                "category": menu_item.get("category")
            }
            order_products.append(dst_it)

        return order_products

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        processed_messages = 0
        timeout: float = 3.0

        while processed_messages < self._batch_size:
            dct_msg = self._consumer.consume(timeout=timeout)

            # Skipping broken messages
            if not dct_msg:
                break
            self._logger.info(f"{datetime.now(timezone.utc)}: New message received")

            # Read the message
            sent_dttm = datetime.fromisoformat(dct_msg.get('sent_dttm'))
            object_id = dct_msg.get('object_id')
            object_type = dct_msg.get('object_type')
            payload = dct_msg.get('payload')

            #Get restaurant info
            restaurant_id = payload.get('restaurant').get('id')
            restaurant_info = self._redis.get(restaurant_id)
            
            #Get user info
            user_id = payload.get('user').get('id')
            user_info = self._redis.get(user_id)

            ordered_items = payload.get('order_items')
            
            out_message = {
                            "object_id": object_id,
                            "object_type": object_type,
                            "payload": {
                                "id": object_id,
                                "date": payload.get("date"),
                                "cost": payload.get("cost"),
                                "payment": payload.get("payment"),
                                "status": payload.get("final_status"),
                                "restaurant": self._format_restaurant(restaurant_id, restaurant_info.get('name')),
                                "user": self._format_user(user_id, user_info.get('name'), user_info.get('login')),
                                "products": self._format_products(ordered_items, restaurant_info),
                            }
            }


            self._stg_repository.order_events_insert(object_id
                                                     ,object_type
                                                     ,sent_dttm
                                                     ,json.dumps(payload,ensure_ascii=False))
            
            self._logger.info(f"{datetime.now(timezone.utc)}: Message saved to DB")
            msg = json.dumps(out_message,ensure_ascii=False)
            self._producer.produce(msg)
            self._logger.info(f"{datetime.now(timezone.utc)}: Message sent to broker")
            processed_messages += 1

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
