from confluent_kafka import Producer
from faker import Faker
import random
import json
import time
from datetime import datetime, timezone

fake = Faker()
# Setting for kafka broker
conf = {"bootstrap.servers": "localhost:9092"}

producer = Producer(conf)

def generate_event():
    return {
        "user_id": f"user_{random.randint(1, 1000)}",
        "webtoon_id": f"toon_{random.randint(1, 20)}",
        "episode_id": f"ep_{random.randint(1, 30)}",
        "action": random.choice(["enter", "scroll", "exit", "complete"]),
        "scroll_ratio": round(random.uniform(0.0, 1.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed : {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    topic = "webtoon-events"

    while True:
        event = generate_event()
        producer.produce(topic, json.dumps(event).encode("utf-8"), callback=delivery_report)
        producer.poll(1)
        time.sleep(1)