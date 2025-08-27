from kafka import KafkaConsumer
import json

class Consumer:
    def __init__(self, topic, broker='localhost:9092', group_id=None):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def read_messages(self):

        for message in self.consumer:
            print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
            print(f"Key: {message.key}, Value: {message.value.decode('utf-8')}")

    def close(self):
        self.consumer.close()

