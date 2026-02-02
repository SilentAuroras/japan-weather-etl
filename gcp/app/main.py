# Standard imports
import logging
import sys

import pandas as pd
from flask import Flask

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forcast

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

def main():

    # Pull station list as dataframe
    logging.info(f'Pulling station list ...')
    generate_stations_list()
    stations = f'data/raw/station-coordinates.parquet'
    stations = pd.read_parquet(stations)

    # Request earthquake data
    logging.info(f'Pulling earthquake data ...')
    get_earthquake_events()

    # Split the dataframe into sections groups for weather request
    logging.info(f'Pulling weather data ...')

    # Pull latitude/longitude from dataframe
    pairs = list(zip(stations['lat'], stations['long']))

    # For each station request weather, loop handled in weather_client
    get_weather_forcast(pairs)

    # Done
    logging.info("Done ...")
    
if __name__ == "__main__":
    main()