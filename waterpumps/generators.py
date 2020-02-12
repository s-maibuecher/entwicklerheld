import arrow
import random
from dateutil import tz
from datetime import datetime


class Generator:
    def test_data_helper_no_error(self):
        base_time = arrow.get(datetime(2020, 2, 12, 9, 00), tz.gettz('Europe/Berlin'))
        for i in range(0, 10_000):
            time = base_time.shift(seconds=i * 30)

            data_berlin = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Berlin",
                'device': 'pump',
                "energy_consumption": 500 + random.randrange(-80, 80)
            }

            data_hamburg = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Hamburg",
                'device': 'pump',
                "energy_consumption": (1200 + random.randrange(-20, 20)) * max(1, (i / 4000))
            }

            data_rain_berlin = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Berlin",
                'device': 'rain_gauge',
                "value": (0 + random.randrange(0, 20)) * random.choice([0, 0, 0, 0, 0, 0, 1])
            }

            data_rain_hamburg = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Hamburg",
                'device': 'rain_gauge',
                "value": (random.randrange(10, 11)) * max(1, (i / 300))
            }

            if i % 120 == 103:
                print(f"{data_rain_berlin},")

            if i % 120 == 27:
                print(f"{data_rain_hamburg},")

            print(f"{data_berlin},")
            print(f"{data_hamburg},")

    def test_data_helper_with_error(self):
        base_time = arrow.get(datetime(2019, 11, 30, 23, 55), tz.gettz('Europe/Berlin'))
        for i in range(0, 15_000):
            time = base_time.shift(seconds=i * 30)

            data_wien = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Vienna",
                'device': 'pump',
                "energy_consumption": (250 + random.randrange(-8, 8)) * max(1, (i / 400))
            }

            data_bremen = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Bremen",
                'device': 'pump',
                "energy_consumption": (2000 + random.randrange(-20, 20)) * max(1, (i / 700))
            }

            data_rain_wien = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Vienna",
                'device': 'rain_gauge',
                "value": (0 + random.randrange(0, 20))
            }

            data_rain_bremen = {
                "time": time.shift(microseconds=random.randrange(1, 499_999)).isoformat(),
                "location": "Bremen",
                'device': 'rain_gauge',
                "value": (10 + random.randrange(-10, 20)) * random.choice([0, 0, 0, 0, 1])
            }

            if i % 120 == 13:
                print(f"{data_rain_wien},")

            if i % 120 == 71:
                print(f"{data_rain_bremen},")

            print(f"{data_wien},")
            print(f"{data_bremen},")