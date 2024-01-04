import json
import sys
import threading

from confluent_kafka import Consumer, KafkaError, KafkaException

from .models import Edits

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

    def run(self):
        print("Inside Logging :  Created Listener ")
        try:
            self.consumer.subscribe([TOPIC])

            while running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    print("----------Got message-----------")
                    message = json.loads(msg.value().decode("utf-8"))
                    self.save(message)
                    print(message)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def save(self, message):
        document = message.get("document", "")
        Edits.objects.create(document=document)
