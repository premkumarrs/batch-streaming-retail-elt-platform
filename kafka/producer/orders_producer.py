from kafka import KafkaProducer
import json
import time
import random


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


statuses = [
    "delivered",
    "shipped",
    "processing",
    "canceled"
]


while True:

    order = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 500),
        "order_status": random.choice(statuses)
    }

    producer.send(
        "retail_orders",
        value=order
    )

    print(f"Sent: {order}")

    time.sleep(2)