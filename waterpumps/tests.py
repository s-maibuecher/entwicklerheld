# Water Pumps Challenge
# This is the IoT-Challenge for the Thinkaton Event of the Smart Systems Hub.

import random
from unittest import TestCase, main
from waterpumps import exmaples, error_examples
from dateutil import tz
from xmlrunner import xmlrunner
import arrow

from waterpumps.task import WaterPumpAnalyzer

from datetime import datetime


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Timer:
    def __init__(self, func_name=""):
        self.func_name = func_name

    def __enter__(self):
        self.start = datetime.now()

    def __exit__(self, *args):
        self.end = datetime.now()
        print("*** {} took {} ***".format(self.func_name, self.end - self.start))


class WaterPumpsTests(TestCase):
    def setUp(self):
        print("##polylith[testStarted")

    def tearDown(self):
        print("##polylith[testFinished")


    def test_scenario_1_saving(self):
        analyzer = WaterPumpAnalyzer()
        data = {'time': '2020-02-10T11:33:00.299356+01:00', 'device': 'pump', 'location': 'Hamburg',
                'energy_consumption': 1269}
        analyzer.handle_message({'time': '2020-02-10T11:33:00.299356+01:00', 'device': 'pump', 'location': 'Hamburg',
                                 'energy_consumption': 1269})
        result = analyzer.get_raw_data(timestamp='2020-02-10T11:33:00.299356+01:00', device="pump",
                                       location="Hamburg")
        self.assertEqual(
            result, 1269,
            f"Expected a energy consumption of {data.get('energy_consumption')} but got {result} for "
            f"get_raw_data(timestamp=\"{data.get('time')}\", device=\"{data.get('device')}\", location=\"{data.get('location')}\")"
        )

        data = {'time': '2020-02-10T11:33:00.299356+01:00', 'device': 'rain_gauge', 'location': 'Dresden', 'value': 47}
        analyzer.handle_message(data)
        result = analyzer.get_raw_data(timestamp='2020-02-10T11:33:00.299356+01:00', device="rain_gauge",
                                       location="Dresden")
        self.assertEqual(
            result, 47,
            f"Expected a energy consumption of {data.get('value')} but got {result} for "
            f"get_raw_data(timestamp=\"{data.get('time')}\", device=\"{data.get('device')}\", location=\"{data.get('location')}\")"
        )

        test_set = exmaples.two_locations_no_error[:500]

        for msg in test_set:
            analyzer.handle_message(msg)
        random.shuffle(test_set)
        with Timer(f"Saving {len(test_set)} messages"):
            for data in test_set:
                result = analyzer.get_raw_data(timestamp=data.get("time"), device=data.get("device"),
                                               location=data.get("location"))
                self.assertEqual(
                    result, data.get('energy_consumption') or data.get('value'),
                    f"Expected a energy consumption of {data.get('energy_consumption')} but got {result} for "
                    f"get_raw_data(timestamp=\"{data.get('time')}\", device=\"{data.get('device')}\", location=\"{data.get('location')}\")"
                )


    def test_scenario_1_analyzing(self):
        # Part 1
        analyzer = WaterPumpAnalyzer()
        for data in exmaples.two_locations_no_error:
            analyzer.handle_message(data)
        start = arrow.get(datetime(2020, 2, 13), tz.gettz('Europe/Berlin')).date()
        end = arrow.get(datetime(2020, 2, 14), tz.gettz('Europe/Berlin')).date()

        with Timer("Berlin"):
            result = analyzer.is_error_mode(start, end, "Berlin")
        self.assertEqual(result, False, f"Expected False but was {result} for Berlin with start {start} and end {end}.")

        with Timer("Hamburg"):
            result = analyzer.is_error_mode(start, end, "Hamburg")
        self.assertEqual(result, False, f"Expected False but was {result} for Hamburg with start {start} and end {end}.")


    def test_scenario_2_analyzing(self):
        # Part 2
        analyzer_2 = WaterPumpAnalyzer()
        for data in error_examples.two_locations_one_error:
            analyzer_2.handle_message(data)
        start = arrow.get(datetime(2020, 2, 3), tz.gettz('Europe/Berlin')).date()
        end = arrow.get(datetime(2020, 2, 4), tz.gettz('Europe/Berlin')).date()

        with Timer("Zuerich"):
            result = analyzer_2.is_error_mode(start, end, "Zuerich")
        self.assertEqual(result, True, f"Expected True but was {result} for Zuerich with start {start} and end {end}.")

        with Timer("Wuppertal"):
            result = analyzer_2.is_error_mode(start, end, "Wuppertal")
        self.assertEqual(result, False, f"Expected False but was {result} for Wuppertal with start {start} and end {end}.")


    def test_scenario_3_analyzing(self):
        # Part 3
        analyzer_3 = WaterPumpAnalyzer()
        for data in error_examples.two_locations_two_errors:
            analyzer_3.handle_message(data)
        start = arrow.get(datetime(2019, 12, 3), tz.gettz('Europe/Berlin')).date()
        end = arrow.get(datetime(2019, 12, 4), tz.gettz('Europe/Berlin')).date()

        with Timer("Vienna"):
            result = analyzer_3.is_error_mode(start, end, "Vienna")
        self.assertEqual(result, True, f"Expected True but was {result} for Vienna with start {start} and end {end}. Energy consumption is higher than 20% of the average, but there was no rain!")

        with Timer("Bremen"):
            result = analyzer_3.is_error_mode(start, end, "Bremen")
        self.assertEqual(result, True, f"Expected True but was {result} for Bremen with start {start} and end {end}. Energy consumption is higher than 20% of the average, but there was no rain!")



if __name__ == '__main__':
    with open('results.xml', 'w') as output:
        main(
            testRunner=xmlrunner.XMLTestRunner(output=output),
            failfast=False, buffer=False, catchbreak=False
        )

