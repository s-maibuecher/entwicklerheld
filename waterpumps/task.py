import datetime
import numpy as np
import arrow


class WaterPumpAnalyzer:

    def __init__(self):
        # Create your storage attributes here.

        # todo besser timedelta nehmen?
        self.data_array = np.zeros((10,), dtype=[('time', 'M'), ('loc', 'U20'), ('is_pump', '?'), ('energy_consumption', 'f8'), ('value', 'i4')])
        self.count_entries = 0
        pass

    def handle_message(self, data: dict):
        ''' erstmal structured arrays ausprobieren und die mit jedem neuen Eintrag erweitern
        Wobei erweitern wohl teurer ist als im vorhinein platz zu reservieren (=preallocating)

        das könnte ich mal mit dem timeit Modul testen

        wobei die Frage halt ist wieviel Platz ich reserviere

        ob eine Datenbank schneller ist?

        print(data)
        {'time': '2020-02-14T22:51:00.082529+01:00', 'location': 'Hamburg', 'device': 'pump', 'energy_consumption': 2248.866}

        oder

        {'time': '2020-02-14T22:51:30.317235+01:00', 'location': 'Berlin', 'device': 'rain_gauge', 'value': 8}

        Fragen die aufkommen:

            Daten abschneiden? Ist wirklich jede Milisekunde nötig? Bringt das was?

            https://stackoverflow.com/a/30330699/2952486

            If that is too slow, why no use an in-memory database? One solution is to use in-memory sqlite3 https://stackoverflow.com/a/21855457/2952486

            you can resize the array in chunks with np.resize, which will still be much faster than the other approach.

            dtype: timedelta lieber als datetime?

            http://chrisschell.de/2018/02/01/how-to-efficiently-deal-with-huge-Numpy-arrays.html

        '''
        is_pump = True if data['device'] == 'pump' else False

        energy_consumption = data['energy_consumption'] if is_pump else 0.0

        value = 0 if is_pump else data['value']

        self.data_array[self.count_entries] = (data['time'], data['location'], is_pump, energy_consumption, value)
        self.count_entries +=1

    def get_raw_data(self, timestamp: str, device: str, location: str) -> dict:
        # Implement this in Scenario 1
        print(self.data_array)
        pass

    def is_error_mode(self, start: datetime.date, end: datetime.date, location: str) -> bool:
        # Implement this in Scenario 2,3 and 4
        pass