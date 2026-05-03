import json

from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    #identifies the consumer group this consumer belongs to - for multiple instances
    "group.id": "order-tracker",
    # tells the consumer what to do if it cannot find where it last left off reading messages
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Consumer is running and subscribed to orders topic")

# cleanly close the connection
try:
    # verifies if theres a new event in the topic it is subscribed
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # extract byte and trasnform to json/python dict
        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("Stopping consumer")

finally:
    consumer.close()