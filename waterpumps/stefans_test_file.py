from waterpumps import exmaples
from waterpumps.task import WaterPumpAnalyzer
import arrow
from datetime import datetime
from dateutil import tz

if __name__ == '__main__':

    # # Level 1:
    # analyzer = WaterPumpAnalyzer()
    #
    # analyzer.handle_message({'time': '2020-02-10T11:33:00.299356+01:00', 'device': 'pump', 'location': 'Hamburg',
    #                          'energy_consumption': 1269})
    #
    # test_set = exmaples.two_locations_no_error[:100]
    #
    # for msg in test_set:
    #     analyzer.handle_message(msg)
    #
    # print(analyzer.pump_data_container)
    # print(analyzer.rain_gauge_data_container)
    #
    # print(analyzer.get_raw_data(timestamp='2020-02-10T11:33:00.299356+01:00', device="pump", location="Hamburg"))
    #
    # print('###'*10)
    # print('Level 2')
    # # Level 2:
    # analyzer = WaterPumpAnalyzer()
    # for data in exmaples.two_locations_no_error:
    #     analyzer.handle_message(data)
    #
    # start = arrow.get(datetime(2020, 2, 13), tz.gettz('Europe/Berlin')).date()
    # end = arrow.get(datetime(2020, 2, 14), tz.gettz('Europe/Berlin')).date()
    #
    # print(analyzer.is_error_mode(start, end, "Berlin"))

    #print(analyzer.is_error_mode(start, end, "Hamburg"))

    analyzer = WaterPumpAnalyzer()
    for data in exmaples.two_locations_no_error:
        analyzer.handle_message(data)
    start = arrow.get(datetime(2020, 2, 13), tz.gettz('Europe/Berlin')).date()
    end = arrow.get(datetime(2020, 2, 14), tz.gettz('Europe/Berlin')).date()

    print(analyzer.is_error_mode(start, end, "Berlin"))

    print(analyzer.is_error_mode(start, end, "Hamburg"))


