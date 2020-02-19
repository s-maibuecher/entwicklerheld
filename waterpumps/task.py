import datetime
from collections import defaultdict
import pickle

'''
        Vorab als kleine Anmerkung: Ich muss zugeben, dass der Code nicht ganz clean ist, ich habe mir aber noch schnell die Mühe gemacht
        ihn an den entscheidenden Stellen zu erläutern.
        Mit "nicht ganz clean" meine ich vor allem das fehlende DRY Prinzip, welches unten bei den Abfragen der
        Durchschnittswerte von energy_consumption und rain_value auffällt.
        Dies entschuldige ich aber aufgrund des Zeitdrucks. Ich hatte nicht viel Zeit für diese Aufgabe und bin durchaus
        in der Lage den Code schön und clean zu refactoren. ;)
        
        Hier zur Aufgabe:  

        Ich arbeite mit der Annahme, dass die Devices keine alten Stati mehr hochschicken:
        dh. ich speicher die Daten fortführend in Tagesintervallen wie folgt ab:

        pump_data_container = {
            'Köln': {
                '20200101' : [(dateiverweis, summe_aller_werte, listen_länge)] # alle Meldungen dieses Tages schon erhalten -> wird aus dem RAM gesourced, um Platz freizumachen
                '20200102' : [(dateiverweis, summe_aller_werte, listen_länge)] # alle Meldungen dieses Tages schon erhalten -> wird aus dem RAM gesourced, um Platz freizumachen
                '20200103' : [(<uhrzeit>, <energie_consumption>), ...], # aktuellster Array wird noch laufend erweitert
            },
            'Düsseldorf': {
                #...
            },
            # ...
        }

        Analog für die Regenpumpen.
        Sobald ein Tag in Gänze abgespeichert wurde habe ich diesen vorsichtshalber aus Platzgründen serialisiert, um den Arbeitsspeicher frei zu machen.
        Man weiß ja nicht wie groß die Validierungsdaten sind. ;)

        Dabei kam ich auf die Idee die Avarage Tages Daten vorab einmal zu berechnen, um hier die Zugriffszeiten zu optimieren.

        Auf Numpy habe ich verzichtet, ich sehe da keinen Vorteil, da die mathematischen Berechnungen sich ja im Rahmen halten
        und die Serialisierung mit reinem Python auch ohne Numpy C-optimiert ist.

        Bin gespannt, ob ich recht behalte...

        Jedenfalls schöner Wettbewerb. Ich hätte mir vielleicht eine laufende Rankingliste gewünscht und damit auch die Möglichkeit sich weiter zu verbessern.

'''

class WaterPumpAnalyzer:

    def __init__(self):
        # Worked with two containers which saved day-wise data for the two different devices:
        self.pump_data_container = defaultdict(dict)
        self.rain_gauge_data_container = defaultdict(dict)

    def handle_message(self, data: dict):

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

            # what is, when the prev day no data was sent? maybe because of a sunday? Then take the prevprevday:
            if date_prev_day not in data_container[stadt_name]:
                date_prev_day = datetime.datetime.strptime(day_string, '%Y%m%d').date() - datetime.timedelta(2)
                date_prev_day = str(date_prev_day).replace('-', '')

            if date_prev_day in data_container[stadt_name]:
                _temp_data_list = data_container[stadt_name][date_prev_day]
                try:
                    # hier vorher noch die sum ausrechnen:
                    _list_data_sum = sum(i[1] for i in _temp_data_list)

                    fname = f'{stadt_name}_{data["device"]}_{date_prev_day}.pkl'
                    with open(fname, 'wb') as f:
                        pickle.dump(_temp_data_list, f)
                    data_container[stadt_name][date_prev_day] = [(fname, _list_data_sum, len(_temp_data_list))]
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


    def get_raw_data(self, timestamp: str, device: str, location: str) -> dict:

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

        # ist es ein lokales gespeichertes Array oder ein Verweis auf eine pickle Datei?
        is_data_serialized = False
        if liste_mit_tageseintraegen[0][0][-4:] == '.pkl':
            is_data_serialized = True

        if is_data_serialized:
            with open(liste_mit_tageseintraegen[0][0], 'rb') as f:
                _temp_list = pickle.load(f)
        else:
            _temp_list = liste_mit_tageseintraegen

        # todo hier ist auf jeden Fall noch Optimierungspotenzial, Sortieralgorithmus...
        result= _temp_list[self.getIndexOfTuple(_temp_list, 0, uhrzeit)][1]

        return result
        #return {}



    def is_error_mode(self, start: datetime.date, end: datetime.date, location: str) -> bool:

        data_container = self.pump_data_container

        # calculate metrics before queried time delta:
        delta = end - start

        overall_consumption_and_sum_entries = (0,0)

        for i in range(delta.days + 1):
            day = start + datetime.timedelta(days=i)
            day = str(day).replace('-', '')

            liste_mit_tageseintraegen = []

            try:
                liste_mit_tageseintraegen = data_container[location][day]
            except:
                # keine Daten vorhanden
                liste_mit_tageseintraegen = [('', 0, 0)]

            # ist es ein lokales gespeichertes Array oder ein Verweis auf eine pickle Datei?
            is_data_serialized = False
            if liste_mit_tageseintraegen[0][0][-4:] == '.pkl':
                is_data_serialized = True

            if is_data_serialized:
                overall_consumption_and_sum_entries = (overall_consumption_and_sum_entries[0]+ liste_mit_tageseintraegen[0][1], overall_consumption_and_sum_entries[1] + liste_mit_tageseintraegen[0][2] )
            else:
                list_data_sum = sum(i[1] for i in liste_mit_tageseintraegen)
                overall_consumption_and_sum_entries = (overall_consumption_and_sum_entries[0]+ list_data_sum, overall_consumption_and_sum_entries[1] +  len(liste_mit_tageseintraegen))


        average_queried_days = float(overall_consumption_and_sum_entries[0])/overall_consumption_and_sum_entries[1]


        # calculate metrics before queried time delta:
        test_end = start - datetime.timedelta(1)
        test_start = test_end - delta

        prev_days_consumption = []

        overall_consumption_and_sum_entries = (0, 0)

        delta_test = test_end - test_start
        for i in range(delta_test.days + 1):
            day = test_start + datetime.timedelta(days=i)
            day = str(day).replace('-', '')

            liste_mit_tageseintraegen = []

            try:
                liste_mit_tageseintraegen = data_container[location][day]
            except:
                # keine Daten vorhanden
                liste_mit_tageseintraegen = [('',0,0)]

            # ist es ein lokales gespeichertes Array oder ein Verweis auf eine pickle Datei?
            is_data_serialized = False
            if liste_mit_tageseintraegen[0][0][-4:] == '.pkl':
                is_data_serialized = True

            if is_data_serialized:
                overall_consumption_and_sum_entries = (
                overall_consumption_and_sum_entries[0] + liste_mit_tageseintraegen[0][1],
                overall_consumption_and_sum_entries[1] + liste_mit_tageseintraegen[0][2])
            else:
                list_data_sum = sum(i[1] for i in liste_mit_tageseintraegen)
                overall_consumption_and_sum_entries = (overall_consumption_and_sum_entries[0] + list_data_sum,
                                                       overall_consumption_and_sum_entries[1] + len(
                                                           liste_mit_tageseintraegen))

        average_prev_queried_days = float(overall_consumption_and_sum_entries[0]) / overall_consumption_and_sum_entries[1]

        #print(average_queried_days, average_prev_queried_days)

        # Energy Consumption went up more than 20%?
        if average_queried_days > average_prev_queried_days * 1.2:

            # so we have to take a look at the rain gauge devices:

            data_container = self.rain_gauge_data_container

            # calculate metrics before queried time delta:
            delta = end - start

            overall_consumption_and_sum_entries = (0, 0)

            for i in range(delta.days + 1):
                day = start + datetime.timedelta(days=i)
                day = str(day).replace('-', '')

                liste_mit_tageseintraegen = []

                try:
                    liste_mit_tageseintraegen = data_container[location][day]
                except:
                    # keine Daten vorhanden
                    liste_mit_tageseintraegen = [('', 0, 0)]

                # ist es ein lokales gespeichertes Array oder ein Verweis auf eine pickle Datei?
                is_data_serialized = False
                if liste_mit_tageseintraegen[0][0][-4:] == '.pkl':
                    is_data_serialized = True

                if is_data_serialized:
                    overall_consumption_and_sum_entries = (
                    overall_consumption_and_sum_entries[0] + liste_mit_tageseintraegen[0][1],
                    overall_consumption_and_sum_entries[1] + liste_mit_tageseintraegen[0][2])
                else:
                    list_data_sum = sum(i[1] for i in liste_mit_tageseintraegen)
                    overall_consumption_and_sum_entries = (overall_consumption_and_sum_entries[0] + list_data_sum,
                                                           overall_consumption_and_sum_entries[1] + len(
                                                               liste_mit_tageseintraegen))

            average_queried_days = float(overall_consumption_and_sum_entries[0]) / overall_consumption_and_sum_entries[
                1]

            # calculate metrics before queried time delta:
            test_end = start - datetime.timedelta(1)
            test_start = test_end - delta

            prev_days_consumption = []

            overall_consumption_and_sum_entries = (0, 0)

            delta_test = test_end - test_start
            for i in range(delta_test.days + 1):
                day = test_start + datetime.timedelta(days=i)
                day = str(day).replace('-', '')

                liste_mit_tageseintraegen = []

                try:
                    liste_mit_tageseintraegen = data_container[location][day]
                except:
                    # keine Daten vorhanden
                    liste_mit_tageseintraegen = [('', 0, 0)]

                # ist es ein lokales gespeichertes Array oder ein Verweis auf eine pickle Datei?
                is_data_serialized = False
                if liste_mit_tageseintraegen[0][0][-4:] == '.pkl':
                    is_data_serialized = True

                if is_data_serialized:
                    overall_consumption_and_sum_entries = (
                        overall_consumption_and_sum_entries[0] + liste_mit_tageseintraegen[0][1],
                        overall_consumption_and_sum_entries[1] + liste_mit_tageseintraegen[0][2])
                else:
                    list_data_sum = sum(i[1] for i in liste_mit_tageseintraegen)
                    overall_consumption_and_sum_entries = (overall_consumption_and_sum_entries[0] + list_data_sum,
                                                           overall_consumption_and_sum_entries[1] + len(
                                                               liste_mit_tageseintraegen))

            average_prev_queried_days = float(overall_consumption_and_sum_entries[0]) / \
                                        overall_consumption_and_sum_entries[1]

            # print(average_queried_days, average_prev_queried_days)

            # Rain also went up more than 20%?
            if not average_queried_days > average_prev_queried_days * 1.2:

                # rain doesn't went up more than 20 % --> seems to be an error!
                return True

        return False