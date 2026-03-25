import geopandas as gpd
import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from pyspark import pipelines as dp
from pyspark.sql import types as T
from retry_requests import retry
from shapely.geometry import Point
from sklearn.cluster import DBSCAN

# Define schema for weather - pulled DDL from weather parquet file
weather_schema = T.StructType([
    T.StructField("name",T.StringType(),nullable=True),
    T.StructField("latitude",T.DoubleType(),nullable=True),
    T.StructField("longitude",T.DoubleType(),nullable=True),
    T.StructField("cluster",T.LongType(),nullable=True),
    T.StructField("temperature_2m",T.DoubleType(),nullable=True),
    T.StructField("is_day",T.DoubleType(),nullable=True),
    T.StructField("precipitation",T.DoubleType(),nullable=True),
    T.StructField("wind_speed_10m",T.DoubleType(),nullable=True),
    T.StructField("wind_direction_10m", T.DoubleType(),nullable=True),
    T.StructField("timestamp",T.TimestampType(),nullable=True),
])

# Create weather table
@dp.table(
    name = 'weather',
    schema = weather_schema
)
def create_weather_table():

    # Read stations from table as pandas
    stations = spark.read.table("stations").toPandas()

    # Call weather function
    df = get_weather_forcast(stations)

    # return spark df
    return spark.createDataFrame(df, schema=weather_schema)

def get_weather_forcast(stations):

    # Create GeoDataFrame from stations list
    gdf = gpd.GeoDataFrame(
        stations,
        geometry=[Point(xy) for xy in zip(stations['longitude'], stations['latitude'])],
        crs="EPSG:4326"
    )

    # Cast CRS to meters
    gdf = gdf.to_crs(epsg=3857)

    # Convert degrees to radians
    coords = np.radians(
        stations[['latitude', 'longitude']].astype(float).values
    )

    # 10km epsilon distance in radians = km / radius of earth
    epsilon = 10 / 6371.0088

    # DBSCAN to find clusters
    db = DBSCAN(
        eps=epsilon,
        # Allow for groups of 1
        min_samples=1,
        metric='haversine'
    )

    # Add a cluster column to group stations by
    stations['cluster'] = db.fit_predict(coords)

    # Sort the clusters, and take the first item for a representative for weather lookup
    cluster_rep = stations.sort_values('cluster').groupby('cluster').first().reset_index()

    # Set up the Open-Meteo API client
    cache_session = requests_cache.CachedSession(backend='memory', expire_after=3600)
    retry_session = retry(cache_session, retries=10, backoff_factor=1)
    open_meteo = openmeteo_requests.Client(session=retry_session)

    # Setup dataframe for all weather forcast requested, includes multiple lat/long locations
    rep_weather = pd.DataFrame()

    # Loop through a list of representatives
    for row in cluster_rep.itertuples():

        # Pull out latitude and longitude
        latitude = row.latitude
        longitude = row.longitude

        # Set up the URL and parameters
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m",
            "models": "jma_seamless",
            "current": [
                "temperature_2m",
                "is_day",
                "precipitation",
                "wind_speed_10m",
                "wind_direction_10m",
            ],
        }

        # Send request to the API
        responses = open_meteo.weather_api(url, params=params)

        # Generate response dictionary
        current = responses[0].Current()
        weather_data = {
            'cluster': row.cluster,  # Cluster ID
            "latitude": latitude,  # Provided latitude
            "longitude": longitude,  # Provided longitude
            "temperature_2m": current.Variables(0).Value(),  # Current temperature
            "is_day": current.Variables(1).Value(),  # Current day or night
            "precipitation": current.Variables(2).Value(),  # Current precipitation
            "wind_speed_10m": current.Variables(3).Value(),  # Current wind speed
            "wind_direction_10m": current.Variables(4).Value(),  # Current wind direction
            'timestamp': pd.Timestamp.utcnow().floor('ms')  # Timestamp for the data
        }

        # Convert the data to a panda frame
        new_data = pd.DataFrame(weather_data, index=[0])

        # Append to the existing dataframe
        rep_weather = pd.concat([rep_weather, new_data], ignore_index=True)

    # Weather columns to keep, drop location and geography for representatives
    weather_columns = [
        'cluster',
        'temperature_2m',
        'is_day',
        'precipitation',
        'wind_speed_10m',
        'wind_direction_10m',
        'timestamp'
    ]

    # Merge weather dataframe back into all stations, assign weather by cluster
    df = stations.merge(rep_weather[weather_columns], on='cluster', how='left')

    # Return
    return df