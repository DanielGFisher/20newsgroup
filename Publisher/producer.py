from kafka import KafkaProducer
from fastapi import FastAPI
from sklearn.datasets import fetch_20newsgroups
import json

class Producer:
    def __init__(self, broker='localhost:9092'):
        self.producer = KafkaProducer(bootstrap=broker)

    def send_message(self, topic, message):
        if isinstance(message, str):
            message = message.encode('utf-8')

        self.producer.send(topic, message)
        self.producer.flush()

    def close(self):
        self.producer.close()