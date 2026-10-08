import logging
import os
import pandas as pd
import re
import time

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forecast

# GCP Imports
from google.cloud import bigquery

# Define headers - pull from env variables
OVERPASS_USER_AGENT = os.getenv("OVERPASS_USER_AGENT")
OVERPASS_REFERER = os.getenv("OVERPASS_REFERER")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "japan_weather_dataset_001")

# Function to write GCS files into BigQuery tables upon creation
def load_parquet_to_bigquery(file_name, table_name, write_disposition):

    # Configure BigQuery client
    client = bigquery.Client()

    # Configure BigQuery table destination and source - GCS > BQ
    table_id = f"{client.project}.{BIGQUERY_DATASET}.{table_name}"
    source_uri = f"gs://{GCS_BUCKET}/raw/{file_name}"

    # Define BQ load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )

    # Load from GCS
    logging.info("Loading %s into %s", source_uri, table_id)
    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)

    # Log results
    load_job.result()
    logging.info("Loaded %s rows into %s", load_job.output_rows, table_id)

# Define main to call helpers and create parquet files
def main():

    # Check env variables exist
    if not OVERPASS_USER_AGENT or not OVERPASS_REFERER:

        # Log error
        logging.error(f'Env variables are not set: OVERPASS_USER_AGENT or OVERPASS_REFERER')

        # Return
        return 1

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

    # Use existing stations list
    else:

        # Read parquet
        stations_df = pd.read_parquet(stations_file)

    # Call function to load stations into BQ
    load_parquet_to_bigquery(
        "station-coordinates.parquet",
        "japan-stations-table-001",
        "WRITE_TRUNCATE",
    )

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

    # Load weather into BQ
    load_parquet_to_bigquery(
        f"weather-{timestamp}.parquet",
        "japan-weather-table-001",
        "WRITE_APPEND",
    )

    # ---------------------------------------------
    # Earthquake
    # ---------------------------------------------
    
    # Request earthquake data
    logging.info(f'Pulling earthquake data...')

    # Call quake function and get df
    quake_df = get_earthquake_events()

    # Check if df empty
    if quake_df.empty:
        logging.error(f'Unable to pull earthquake events')

    # Create a parquet file locally
    else:

        # Clean up column names
        quake_df.columns = [
            re.sub(r"[^a-zA-Z0-9_]", "_", column).lower()
            for column in quake_df.columns
        ]

        # Convert df to parquet
        quake_df.to_parquet(f'data/raw/quake-{timestamp}.parquet', index=False)

        # Load Earthquake events into BQ
        load_parquet_to_bigquery(
            f"quake-{timestamp}.parquet",
            "japan-earthquake-table-001",
            "WRITE_APPEND",
        )

    # ---------------------------------------------
    # Summary
    # ---------------------------------------------
    
    # Done
    logging.info("Done...")

    # Return timestamp for tests/logging
    return timestamp
    
if __name__ == "__main__":
    main()