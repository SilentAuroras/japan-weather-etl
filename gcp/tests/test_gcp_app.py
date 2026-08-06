import pandas as pd
import sys
import unittest
from pathlib import Path

# Fix relative import path
root = Path(__file__).resolve().parents[1]
app_path = 'app'
sys.path.insert(0, str(app_path))

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forecast
from main import main

# Define class for testing gcp docker image
class TestGcpApp(unittest.TestCase):

    # Define locations for test class - Tokyo Station and Kyoto
    coordinates = [(35.675163966, 139.766830266), (34.98561, 135.758915)]

    # Station locations generator test
    def test_stations(self):

        # Call function
        df = generate_stations_list()

        # Check that returns dataframe
        self.assertIsInstance(df, pd.DataFrame)

        # Check df is not empty > 50
        self.assertGreater(len(df), 50)

        # Print
        print(f'[TEST] - Stations API returned station count: {len(df)}')

    # Weather Client Tests
    def test_weather(self):

        # Call weather function
        df = get_weather_forecast(self.coordinates)

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

    # Test main - all files created
    def test_main(self):

        # Call main and grab timestamp
        timestamp = main()

        # Check stations parquet created
        stations_path = Path("data/raw/station-coordinates.parquet")
        self.assertTrue(stations_path.is_file())

        # Check weather parquet created
        weather_path = Path(f'data/raw/weather-{timestamp}.parquet')
        self.assertTrue(weather_path.is_file())

        # Check quake parquet created
        quake_path = Path(f'data/raw/quake-{timestamp}.parquet')
        self.assertTrue(quake_path.is_file())