import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(0)

events = ["page_view", "click", "signup", "logout"]
user_ids = [fake.uuid4() for _ in range(100)]

data = []

for _ in range(1000):
    event_time = fake.date_time_between(start_date="-7d", end_date="now")
    data.append({
        "user_id": random.choice(user_ids),
        "event": random.choice(events),
        "event_time": event_time.isoformat()
    })

df = pd.DataFrame(data)
df.to_csv("user_event_log.csv", index=False)