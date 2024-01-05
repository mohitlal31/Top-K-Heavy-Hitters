import json
import sys
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from probables import countminsketch

from .models import CountMinSketchKeys, CountMinSketchModel

TOPIC = "topic_document_edit_created"
CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "logger_group",
    "auto.offset.reset": "smallest",
}

running = True


class ConsumerEditCreated(threading.Thread):
    def __init__(self) -> None:
        threading.Thread.__init__(self)
        self.consumer = Consumer(CONF)
        self.last_process_time = time.time()
        self.cms = countminsketch.CountMinSketch(width=1000, depth=5)
        self.keys = set()

    def run(self):
        print("Inside Logging :  Created Listener ")
        try:
            self.consumer.subscribe([TOPIC])

            while running:
                msg = self.consumer.poll(timeout=0.1)
                if msg is None:
                    continue

                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                elif msg.error():
                    print(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
                else:
                    print("----------Got message-----------")
                    message = json.loads(msg.value().decode("utf-8"))
                    self.process_message(message)
                    print(message)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def process_message(self, message):
        document = message.get("document", "")
        current_time = time.time()
        if current_time - self.last_process_time < 10:
            self.cms.add(document)
            self.keys.add(document)
        else:
            count_min_sketch_instance = CountMinSketchModel()
            count_min_sketch_instance.set_count_min_sketch(self.cms)
            count_min_sketch_instance.save()

            keys_instance = CountMinSketchKeys()
            keys_instance.count_min_sketch = count_min_sketch_instance
            keys_instance.set_keys(list(self.keys))
            keys_instance.save()

            self.cms.clear()
            self.keys.clear()
            self.last_process_time = time.time()
