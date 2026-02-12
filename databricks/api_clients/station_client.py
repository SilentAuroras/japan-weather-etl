# JR Station Location Generator
# This script generates a list of train stations in Japan with their respective latitude and longitude

import dlt
import overpy
import pandas as pd

# Write dataframe to DLT table - japan_train_stations
@dlt.table(
    name="japan_train_stations"
)
def generate_stations_list():
    
    # Create Overpass API object
    api = overpy.Overpass()

    # Query train stations in Japan using ISO 3166-1 code for Japan (JP)
    query = f'''
    [out:json][timeout:180];
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
        latitude = float(node.lat)
        longitude = float(node.lon)
        if name:
            stations.append({"name": name, "lat": latitude, "long": longitude})

    # Generate dataframe for the results
    return spark.createDataFrame(stations)