import pandas as pd
import sys
import unittest
from pathlib import Path

# Fix relative import path
root = Path(__file__).resolve().parents[1]
app_path = root / 'app'
sys.path.insert(0, str(app_path))

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forecast

# Define class for testing gcp docker image
class TestGcpApp(unittest.TestCase):

    # Weather Client Tests
    def test_weather(self):

        # Weather client expects a stations dataframe with latitude/longitude
        stations = generate_stations_list()

        print(f'[TEST] - Stations API returned station count: {len(stations)}')

        # Call weather function
        df = get_weather_forecast(stations)

        # Check that returns dataframe
        self.assertIsInstance(df, pd.DataFrame)

        # Check df is not empty > 50
        self.assertGreater(len(df), 50)

        # Print
        print(f'[TEST] - Weather API returned count: {len(df)}')

    # Test earthquake
    def test_quake(self):

        # Call quake function
        df = get_earthquake_events()

        # Check that returns dataframe
        self.assertIsInstance(df, pd.DataFrame)

        # Check that df is not empty
        self.assertGreater(len(df), 0)

        # Print
        print(f'[TEST] - Quake API returned count: {len(df)}')

if __name__ == '__main__':
    unittest.main()