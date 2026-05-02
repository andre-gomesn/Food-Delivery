import json
import uuid

from confluent_kafka import Producer

# bootstrap.servers is kafka using the address to discover old brokers in the server
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery falied: {err}")
    else:
        print(f"Delivered: {msg.value().decode('utf-8')}")
        print(f"Delivered to: {msg.topic()}: partition: {msg.partition()}: at offset: {msg.offset()}")

order = {
    # uuid python built in library, in the code it generates a random unique identifier
    "order_id": str(uuid.uuid4()),
    "user": "andre",
    "item": "chicken pizza",
    "quantity": 1
}

# convert dictionary/object to string representation and encode into byte format, which kafka understands
value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)
producer.flush()

# check if the topic was created and what events are inside (ignore things inside paranteses)
# docker exec -it kafka(name of the container) kafka-topics(kafka cli) --list --bootstrap-server localhost:9092
# can use --describe instead of --list
# docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders