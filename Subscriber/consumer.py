from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime

class Consumer:
    def __init__(self, topic, broker='kafka:9092', group_id=None, mongo_url="mongodb://mongo:27017/"):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.client = MongoClient(mongo_url)
        self.db = self.client["newsgroups"]
        self.collection = self.db[f"{topic}_messages"]

    def consume_messages(self):
        for message in self.consumer:
            msg = message.value
            msg["timestamp"] = datetime.utcnow().isoformat()
            self.collection.insert_one(msg)

    def get_messages(self):
        return list(self.collection.find({}, {"_id": 0}))
