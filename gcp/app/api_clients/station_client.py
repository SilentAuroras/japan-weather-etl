# JR Station Location Generator
# This script generates a list of train stations in Japan with their latitude and longitude

import overpy
import pandas as pd
import os

def generate_stations_list():
    """
    Generate a list of train stations.

    Return: none
    """

    # Create data folders
    if not os.path.exists('data/raw'):
        os.makedirs('data/raw')

    # Create Overpass API object
    api = overpy.Overpass()
    
    # Query rail stations in Japan using ISO 3166-1 code for Japan (JP)
    # Filter on station > train=yes
    query = """
    [out:json][timeout:60];
    area["ISO3166-1"="JP"][admin_level=2]->.searchArea;
    nwr["public_transport"="station"]["train"="yes"](area.searchArea);
    out geom;
    """
       
    # Send the request
    results = api.query(query)

    # Iterate over nodes to pull station name, latitude, longitude
    stations = []
    for node in results.nodes:
        name = node.tags.get("name")
        latitude = node.lat
        longitude = node.lon
        if name:
            stations.append({"name": name, "lat": latitude, "long": longitude})
    
    # Generate dataframe for the results
    df = pd.DataFrame(stations)

    # Save to parquet file
    # Format: station name, latitude, longitude
    df.to_parquet("data/raw/station-coordinates.parquet", index=False)