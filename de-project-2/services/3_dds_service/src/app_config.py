import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from lib.redis import RedisClient


class AppConfig:
    
    DEFAULT_JOB_INTERVAL = 25
    DEFAULT_BATCH_SIZE = 100

    def __init__(self) -> None:

        self.kafka_host = str(os.getenv('KAFKA_HOST') or "")
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')) or 0)
        self.kafka_consumer_group = str(os.getenv('KAFKA_CONSUMER_GROUP') or "")
        self.kafka_consumer_topic = str(os.getenv('KAFKA_SOURCE_TOPIC') or "")
        self.kafka_producer_topic = str(os.getenv('KAFKA_DESTINATION_TOPIC') or "")

        self.pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST') or "")
        self.pg_warehouse_port = int(str(os.getenv('PG_WAREHOUSE_PORT') or 0))
        self.pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME') or "")
        self.pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER') or "")
        self.pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD') or "")

    def kafka_producer(self):
        return KafkaProducer(
            host= self.kafka_host,
            port= self.kafka_port,
            topic= self.kafka_producer_topic,
        )

    def kafka_consumer(self):
        return KafkaConsumer(
            host = self.kafka_host,
            port= self.kafka_port,
            topic= self.kafka_consumer_topic,
            group= self.kafka_consumer_group,
        )

    def pg_warehouse_db(self):
        return PgConnect(
            host= self.pg_warehouse_host,
            port= self.pg_warehouse_port,
            db_name= self.pg_warehouse_dbname,
            user= self.pg_warehouse_user,
            pw= self.pg_warehouse_password
        )
