import json
import socket

from confluent_kafka import Producer

TOPIC = "topic_document_edit_created"
CONF = {
    "bootstrap.servers": "localhost:9092",
    "client.id": socket.gethostname(),
}


class ProducerEditCreated:
    def __init__(self) -> None:
        self.producer = Producer(CONF)

    def publish(self, message) -> None:
        print("------- Publish to kafka ---------")
        print(f"message = {message}")
        self.producer.produce(
            TOPIC, key="key.doc_edit.created", value=json.dumps(message)
        )
