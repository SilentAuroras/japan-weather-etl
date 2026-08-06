import logging
import os
import pandas as pd
import time

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forecast

# Define main to call helpers and create parquet files
def main():

    # ---------------------------------------------
    # Stations
    # ---------------------------------------------
    
    # Pull station list as dataframe
    logging.info(f'Pulling station list...')

    # Define stations filename
    stations_file = f'data/raw/station-coordinates.parquet'

    # Generate list if stations list does not already exist
    if not os.path.exists(stations_file):

        # Call function
        stations_df = generate_stations_list()

        # Generate stations parquet
        stations_df.to_parquet("data/raw/station-coordinates.parquet", index=False)

    else:

        # Read parquet
        stations_df = pd.read_parquet(stations_file)

    # ---------------------------------------------
    # Weather
    # ---------------------------------------------
    
    # Pull weather data and generate parquet
    logging.info(f'Pulling weather data...')

    # Call weather function
    weather_df = get_weather_forecast(stations_df)

    # Generate timestamp for filename
    timestamp = time.strftime("%Y%m%d-%H%M%S")

    # Create a parquet file locally
    weather_df.to_parquet(f'data/raw/weather-{timestamp}.parquet', index=False)

    # ---------------------------------------------
    # Earthquake
    # ---------------------------------------------
    
    # Request earthquake data
    logging.info(f'Pulling earthquake data...')

    # Call quake function and get df
    quake_df = get_earthquake_events()

    # Create a parquet file locally
    quake_df.to_parquet(f'data/raw/quake-{timestamp}.parquet', index=False)

    # ---------------------------------------------
    # Summary
    # ---------------------------------------------
    
    # Done
    logging.info("Done...")

    # Return timestamp for tests/logging
    return timestamp
    
if __name__ == "__main__":
    main()