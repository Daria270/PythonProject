import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    topics="register-events",
    bootstrap_servers=["185.185.143.231:8085"],
    auto_offset_reset="earliest",
    # value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for message in consumer:
    print(message.value)

