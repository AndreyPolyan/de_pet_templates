from kafka import KafkaProducer
import json


class KafkaProducerObj:
    def __init__ (self, bootstrap_servers, sasl_username, sasl_password, ca_location, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ca_location = ca_location
        
    def create_kafka_producer(self) -> KafkaProducer:
        """
        Creates a Kafka producer with SASL_SSL authentication.
        
        :param bootstrap_servers: Kafka server address .
        :param sasl_username: Username for SASL authentication.
        :param sasl_password: Password for SASL authentication.
        :param ca_location: Path to the CA certificate file.
        :return: Configured KafkaProducer instance.
        """
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=self.sasl_username,
            sasl_plain_password=self.sasl_password,
            ssl_cafile=self.ca_location,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')  # Serialize keys if required
        )
    
    @staticmethod
    def send_message(producer: KafkaProducer, topic, key, value) -> None:
        """
        Sends a message to the specified Kafka topic.

        :param producer: KafkaProducer instance.
        :param topic: Target Kafka topic.
        :param key: Message key (used for partitioning).
        :param value: Message value (payload).
        """
        producer.send(topic, key=key, value=value)
        producer.flush()
        print(f"Sent message with key: {key}, value: {value}")