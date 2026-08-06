from pyspark import pipelines as dp
from pyspark.sql import types as T

# Define stations schema
SCHEMA = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("longitude", T.DoubleType(), True),
])

# Define table for stations list - read from parquet due to OSM api restrictions
@dp.table(
    name="stations",
    schema=SCHEMA
)
def generate_stations_list():
    """
    Load and parse station list parquet files with year metadata.
    :return: stations_df: dataframe of all stations within a given year (name, latitude, longitude, year)
    """

    # Read in parquet file to dataframe
    df = (
        spark.read
            .format("parquet")
            .load("/Volumes/japan_weather/weather_changes/public_datasets/stations-list-2025.parquet")
    )

    # Return dataframe
    return df