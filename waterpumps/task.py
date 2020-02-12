import datetime
import numpy as np
import arrow


class WaterPumpAnalyzer:

    def __init__(self):
        # Create your storage attributes here.

        pass

    def handle_message(self, data: dict):
        # This method gets called with the raw data. Implement this in Scenario 1.
        pass

    def get_raw_data(self, timestamp: str, device: str, location: str) -> dict:
        # Implement this in Scenario 1
        pass

    def is_error_mode(self, start: datetime.date, end: datetime.date, location: str) -> bool:
        # Implement this in Scenario 2,3 and 4
        pass