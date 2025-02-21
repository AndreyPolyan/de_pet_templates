import yaml
import time

from lib import CampaignRecordGenerator, KafkaProducerObj

gen = CampaignRecordGenerator()

with open("/root/project8/credentials.yaml", "r") as file:
    config = yaml.safe_load(file)

source_kafka = KafkaProducerObj(**{key.lower(): value for key,value in config['KafkaSourceWrite'].items()})

source_kafka_client = source_kafka.create_kafka_producer()

if __name__ == '__main__':
    for i in range(100):
        KafkaProducerObj.send_message(source_kafka_client, config['KafkaSourceWrite']['TOPIC'], **gen.generate_record())
        time.sleep(1)