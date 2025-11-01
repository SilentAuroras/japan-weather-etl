# Standard imports
import os.path
import pandas as pd

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forcast

# Main function
def main():

    # Call extract to generate parquet files
    extract()

def extract():

    # Pull station list as dataframe
    print(f'Pulling station list ...')
    generate_stations_list()
    stations = f'data/raw/station-coordinates.parquet'
    stations = pd.read_parquet(stations)

    # Request earthquake data
    print(f'Pulling earthquake data ...')
    get_earthquake_events()

    # Split the dataframe into sections groups for weather request
    print(f'Pulling weather data ...')

    # Pull latitude/longitude from dataframe
    pairs = list(zip(stations['lat'], stations['long']))

    # For each station request weather, loop handled in weather_client
    get_weather_forcast(pairs)

if __name__ == "__main__":
    main()