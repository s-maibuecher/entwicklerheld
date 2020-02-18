import datetime
import numpy as np
import arrow
from collections import defaultdict
import pickle
from functools import reduce

class WaterPumpAnalyzer:

    def __init__(self):
        # Create your storage attributes here.

        self.pump_data_container = defaultdict(dict)
        self.rain_gauge_data_container = defaultdict(dict)

        # self.data_array = np.zeros((10000,), dtype=[('time', 'datetime64[s]'), ('loc', 'U20'), ('is_pump', '?'), ('energy_consumption', 'f8'), ('value', 'i4')])
        # self.count_entries = 0
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
        # is_pump = True if data['device'] == 'pump' else False
        #
        # energy_consumption = data['energy_consumption'] if is_pump else 0.0
        #
        # value = 0 if is_pump else data['value']
        #
        # try:
        #     self.data_array[self.count_entries] = (np.datetime64(data['time']), data['location'], is_pump, energy_consumption, value)
        # except IndexError:
        #     print('###'*10)
        #     print(self.data_array.shape)
        #     shape_ = self.data_array.shape[0]
        #     self.data_array.resize((shape_ + 100000,), refcheck=False )
        #     self.data_array[self.count_entries] = (
        #         np.datetime64(data['time']), data['location'], is_pump, energy_consumption, value)
        # self.count_entries +=1
        #
        # # --> https://stackoverflow.com/a/12285656/2952486
        #
        # ### HIER WEITER ######### --> ich glaube ein dict mit Städte Keys, value ist eine Datei, die als Einträge die einzelnen Tage hat

        '''
        Ich arbeite mit der Annahme, dass die Devices keine alten Stati mehr hochschicken:
        dh. ich speicher die Daten fortführend in Tagesintervallen wie folgt ab:
        
        pump_data_container = {
            'Köln': {
                '20200101' : dateiverweis, # alle Meldungen dieses Tages schon erhalten -> wird aus dem RAM gesourced, um Platz freizumachen
                '20200102' : dateiverweis, # alle Meldungen dieses Tages schon erhalten -> wird aus dem RAM gesourced, um Platz freizumachen
                '20200103' : [(<uhrzeit>, <energie_consumption>), ...], # aktuellster Array wird noch laufend erweitert
            },
            'Düsseldorf': {
                #...
            },
            # ...
        }
        
        '''

        is_pump = True if data['device'] == 'pump' else False
        stadt_name = data['location']
        time_data = data['time']

        day_string = time_data[:10].replace('-', '')


        if is_pump:
            data_container = self.pump_data_container
            data_value = data['energy_consumption']
        else: # -> rain_gauge
            data_container = self.rain_gauge_data_container
            data_value = data['value']

        if not day_string in data_container[stadt_name]: # True, wenn der Tag noch nicht angelegt wurde
            # dann leg den Tag an:
            data_container[stadt_name][day_string] = [(time_data[10:], data_value)]

            date_prev_day = datetime.datetime.strptime(day_string, '%Y%m%d').date() - datetime.timedelta(1)
            date_prev_day= str(date_prev_day).replace('-', '')

            # todo was ist, wenn der Vortag keine Daten hatte, wegen Sonntag oder so...

            if date_prev_day in data_container[stadt_name]:
                _temp_data_list = data_container[stadt_name][date_prev_day]
                try:
                    # hier vorher noch avarage ausrechnen:
                    _list_data_avarage = sum(i[1] for i in _temp_data_list)/float(len(_temp_data_list))

                    fname = f'{stadt_name}_{data["device"]}_{date_prev_day}.pkl'
                    with open(fname, 'wb') as f:
                        pickle.dump(_temp_data_list, f)
                    _temp_data_list = (fname, _list_data_avarage)
                except:
                    pass

        else:
            # Tag existiert schon:
            data_container[stadt_name][day_string].append((time_data[10:], data_value))

    # https://stackoverflow.com/a/947184/2952486
    # Stops iterating through the list as soon as it finds the value
    def getIndexOfTuple(self, l, index, value):
        for pos, t in enumerate(l):
            if t[index] == value:
                return pos

        # Matches behavior of list.index
        raise ValueError("list.index(x): x not in list")

        # getIndexOfTuple(tuple_list, 0, "cherry")  # = 1

    def get_raw_data(self, timestamp: str, device: str, location: str) -> dict:
        # Implement this in Scenario 1
        # Beispielaufruf: get_raw_data(timestamp='2020-02-10T11:33:00.299356+01:00', device="pump",
        #                                        location="Hamburg")

        is_pump = True if device == 'pump' else False
        stadt_name = location
        time_data = timestamp

        day_string = time_data[:10].replace('-', '')
        uhrzeit = time_data[10:]

        if is_pump:
            data_container = self.pump_data_container

        else:  # -> rain_gauge
            data_container = self.rain_gauge_data_container


        # hier die Abfrage:
        liste_mit_tageseintraegen = data_container[stadt_name][day_string]

        # --> hier weiter: ist es ein lokales Array oder ein Verweis auf eine pickle Datei?

        # todo hier ist auf jeden Fall noch Optimierungspotenzial, Sortieralgorithmus...
        result= liste_mit_tageseintraegen[self.getIndexOfTuple(liste_mit_tageseintraegen, 0, uhrzeit)][1]

        return result
        #return {}



    def is_error_mode(self, start: datetime.date, end: datetime.date, location: str) -> bool:
        # Implement this in Scenario 2,3 and 4
        #print(start, end)

        delta = end - start

        for i in range(delta.days + 1):
            day = start + datetime.timedelta(days=i)
            print(day)

        pass