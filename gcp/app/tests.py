# General Imports
import pandas as pd
import os
import requests

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forcast

# Station locations generator test
station_filename = f"data/raw/station-coordinates.parquet"
if os.path.exists(station_filename):
    print('station coordinates file exists')
else:
    generate_stations_list()
    assert os.path.exists(station_filename), "station-coordinates.parquet not created"
df = pd.read_parquet(station_filename)
print(df)

# Weather Client Tests - Tokyo Station and Kyoto
coordinates = [(35.675163966, 139.766830266), (34.98561, 135.758915)]
weather_uuid = get_weather_forcast(coordinates)
weather_filename = f"data/raw/weather_{weather_uuid}.parquet"
assert os.path.exists(weather_filename), f"{weather_filename} not created"
df = pd.read_parquet(weather_filename)
print(df)

# Earthquake Client Tests
quake_uuid = get_earthquake_events()
quake_filename = f"data/raw/quake_{quake_uuid}.parquet"
assert os.path.exists(quake_filename), f"{quake_filename} not created"
df = pd.read_parquet(quake_filename)
print(df)