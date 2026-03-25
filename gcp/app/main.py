# Standard imports
import logging
import os.path
import sys
import pandas as pd

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forecast

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

def main():

    # Pull station list as dataframe
    logging.info(f'Pulling station list...')
    stations = f'data/raw/station-coordinates.parquet'

    # Generate list if stations list does not already exist
    if not os.path.exists(stations):
        generate_stations_list()

    # Read in stations list
    stations = pd.read_parquet(stations)

    # Request earthquake data
    logging.info(f'Pulling earthquake data...')
    get_earthquake_events()

    # Split the dataframe into sections groups for weather request
    logging.info(f'Pulling weather data...')
    get_weather_forecast(stations)

    # Done
    logging.info("Done...")
    
if __name__ == "__main__":
    main()