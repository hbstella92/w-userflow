import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from faker import Faker

NUM_USERS = 3
MAX_SESSIONS_PER_USER = 3
MAX_EPISODES_PER_SESSION = 3

REFERRERS = ["recommend", "search", "banner"]
DEVICE_TYPES = ["Android", "iOS", "Tablet", "Desktop"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
APP_VERSIONS = ["1.0.0", "1.2.3", "2.0.0"]
COUNTRIES = ["KR", "US", "JP", "TW", "TH", "ID", "VN", "FR", "ES", "DE"]


def generate_events(user_id, signup_date):
    events = []
    current_time = signup_date + timedelta(days=random.randint(1, 10))
    last_visit_date = None

    for _ in range(random.randint(1, MAX_SESSIONS_PER_USER)):
        session_id = str(uuid.uuid4())

        session_device = random.choice(DEVICE_TYPES)
        session_app_version = random.choice(APP_VERSIONS)
        session_country = random.choice(COUNTRIES)
        session_referrer = random.choice(REFERRERS)

        episode_counts = random.randint(1, MAX_EPISODES_PER_SESSION)

        for ep_idx in range(episode_counts):
            webtoon_id = f"webtoon_{random.randint(1, 10)}"
            episode_id = f"{webtoon_id}_ep_{random.randint(1, 50)}"
            scroll_ratio = round(random.uniform(0.1, 1.0), 2)
            duration = random.randint(30, 180)  # second 단위
            is_returning = random.random() < 0.2
            thumbnail_clicked = random.random() < 0.8

            # enter
            events.append(
                {
                    "user_id": user_id,
                    "signup_date": signup_date.date().isoformat(),
                    "last_date": (
                        last_visit_date.isoformat() if last_visit_date else None
                    ),
                    "session_id": session_id,
                    "timestamp": current_time.isoformat(),
                    "event_type": "enter",
                    "referrer": session_referrer,
                    "device_type": session_device,
                    "app_version": session_app_version,
                    "country": session_country,
                    "webtoon_id": webtoon_id,
                    "episode_id": episode_id,
                    "episode_index_in_session": ep_idx + 1,
                    "scroll_ratio": 0.0,
                    "duration": 0,
                    "dropoff_position": None,
                    "is_returning_episode": is_returning,
                    "thumbnail_clicked": thumbnail_clicked,
                }
            )

            current_time += timedelta(seconds=random.randint(10, 30))

            # scroll
            events.append(
                {
                    "user_id": user_id,
                    "signup_date": signup_date.date().isoformat(),
                    "last_date": (
                        last_visit_date.isoformat() if last_visit_date else None
                    ),
                    "session_id": session_id,
                    "timestamp": current_time.isoformat(),
                    "event_type": "scroll",
                    "referrer": session_referrer,
                    "device_type": session_device,
                    "app_version": session_app_version,
                    "country": session_country,
                    "webtoon_id": webtoon_id,
                    "episode_id": episode_id,
                    "episode_index_in_session": ep_idx + 1,
                    "scroll_ratio": scroll_ratio,
                    "duration": duration,
                    "dropoff_position": None,
                    "is_returning_episode": is_returning,
                    "thumbnail_clicked": thumbnail_clicked,
                }
            )

            current_time += timedelta(seconds=duration)

            # complete or exit
            event_type = "complete" if scroll_ratio >= 0.9 else "exit"
            dropoff_position = scroll_ratio if event_type == "exit" else None
            events.append(
                {
                    "user_id": user_id,
                    "signup_date": signup_date.date().isoformat(),
                    "last_date": (
                        last_visit_date.isoformat() if last_visit_date else None
                    ),
                    "session_id": session_id,
                    "timestamp": current_time.isoformat(),
                    "event_type": event_type,
                    "referrer": session_referrer,
                    "device_type": session_device,
                    "app_version": session_app_version,
                    "country": session_country,
                    "webtoon_id": webtoon_id,
                    "episode_id": episode_id,
                    "episode_index_in_session": ep_idx + 1,
                    "scroll_ratio": scroll_ratio,
                    "duration": duration,
                    "dropoff_position": dropoff_position,
                    "is_returning_episode": is_returning,
                    "thumbnail_clicked": thumbnail_clicked,
                }
            )

            current_time += timedelta(minutes=random.randint(2, 5))

        last_visit_date = current_time.date()
        current_time += timedelta(minutes=35)

    return events


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed : {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    fake = Faker()

    producer = Producer({"bootstrap.servers": "localhost:9092"})
    topic = "webtoon-events"

    while True:
        for i in range(NUM_USERS):
            user_id = f"user_{i + 1}"
            signup_date = datetime.utcnow() - timedelta(days=random.randint(10, 30))

            user_events = generate_events(user_id, signup_date)

            for e in user_events:
                producer.produce(
                    topic, json.dumps(e).encode("utf-8"), callback=delivery_report
                )
                producer.poll(1)

            time.sleep(1)
