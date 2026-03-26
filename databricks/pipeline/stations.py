import overpy
from pyspark import pipelines as dp
from pyspark.sql import types as T

# Define stations schema
SCHEMA = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("longitude", T.DoubleType(), True),
])

# Define table for stations list
@dp.table(
    name="stations",
    schema=SCHEMA
)
def generate_stations_list():
    """
    Query all rail stations in Japan via the Overpass API

    :return: Spark DataFrame with columns (name, latitude, longitude) for each rail station in Japan
    """

    # Create Overpass API object
    api = overpy.Overpass(
        max_retry_count=10,
        retry_timeout=60
    )

    # Query rail stations in Japan using ISO 3166-1 code for Japan (JP)
    query = f'''
    [out:json][timeout:120];
    area["ISO3166-1"="JP"]["admin_level"="2"]->.searchArea;
    node
      ["railway"="station"]
      ["public_transport"="station"]
      (area.searchArea);
    out;
    '''

    # Send the request
    results = api.query(query)

    # Iterate over nodes to pull station name, latitude, longitude
    stations = []
    for node in results.nodes:
        name = node.tags.get("name")
        latitude = node.lat
        longitude = node.lon
        if name:
            stations.append({
                "name": name,
                "latitude": float(latitude),
                "longitude": float(longitude),
            })

    # Generate dataframe for the results
    return spark.createDataFrame(stations, schema=SCHEMA)