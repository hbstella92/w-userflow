from confluent_kafka import Producer
from faker import Faker
import random
import json
import time
import uuid
from datetime import datetime, timezone

fake = Faker()
# Setting for kafka broker
conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)

device_types = ["mobile", "tablet", "desktop"]
browsers = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
COUNTRIES = ["KR", "US", "JP", "TH", "ID", "BR", "TW", "FR"]

def generate_events():
    session_id = str(uuid.uuid4())
    user_id = fake.uuid4()
    webtoon_id = f"toon_{random.randint(1, 20)}"
    episode_id = f"ep_{random.randint(1, 30)}"
    country_code = random.choice(COUNTRIES)
    ip_address = fake.ipv4_public()
    device_type = random.choice(device_types)
    browser = random.choice(browsers)
    timestamp_base = datetime.now(timezone.utc)

    is_complete = random.random() < 0.6

    sequence = [
        {
            "session_id": session_id,
            "user_id": user_id,
            "webtoon_id": webtoon_id,
            "episode_id": episode_id,
            "action": "enter",
            "scroll_ratio": 0.0,
            "timestamp": timestamp_base.isoformat(),
            "country_code": country_code,
            "ip_address": ip_address,
            "device_type": device_type,
            "browser": browser
        },
        {
            "session_id": session_id,
            "user_id": user_id,
            "webtoon_id": webtoon_id,
            "episode_id": episode_id,
            "action": "scroll",
            "scroll_ratio": round(random.uniform(0.3, 1.0), 2),
            "timestamp": datetime.fromtimestamp(timestamp_base.timestamp() + 2, tz=timezone.utc).isoformat(),
            "country_code": country_code,
            "ip_address": ip_address,
            "device_type": device_type,
            "browser": browser
        },
        {
            "session_id": session_id,
            "user_id": user_id,
            "webtoon_id": webtoon_id,
            "episode_id": episode_id,
            "action": "complete" if is_complete else "exit",
            "scroll_ratio": 1.0 if is_complete else round(random.uniform(0.0, 0.6), 2),
            "timestamp": datetime.fromtimestamp(timestamp_base.timestamp() + 4, tz=timezone.utc).isoformat(),
            "country_code": country_code,
            "ip_address": ip_address,
            "device_type": device_type,
            "browser": browser
        }
    ]

    return sequence

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed : {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    topic = "webtoon-events"

    while True:
        events = generate_events()

        for e in events:
            producer.produce(topic, json.dumps(e).encode("utf-8"), callback=delivery_report)
            producer.poll(1)

        time.sleep(1)