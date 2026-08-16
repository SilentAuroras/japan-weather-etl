import os
import overpy
import pandas as pd
import requests

# Define headers
OVERPASS_USER_AGENT = os.getenv("OVERPASS_USER_AGENT")
OVERPASS_REFERER = os.getenv("OVERPASS_REFERER")

# Generate a list of train stations in Japan with their latitude and longitude
def generate_stations_list():

    # Create Overpass API object with user-agent to fix 406 issue
    api = overpy.Overpass()

    # Query rail stations in Japan using ISO 3166-1 code for Japan (JP)
    query = f'''
    [out:json][timeout:180];
    area["ISO3166-1"="JP"]["admin_level"="2"]->.searchArea;
    node
      ["railway"="station"]
      ["public_transport"="station"]
      (area.searchArea);
    out;
    '''
    
    # Build headers
    headers = {
        "User-Agent": str(OVERPASS_USER_AGENT),
        "Referer": str(OVERPASS_REFERER),
        "Accept": "application/json",
    }

    # Send request with headers
    response = requests.post(
        url = "https://overpass-api.de/api/interpreter",
        data={ "data": query },
        headers = headers,
        timeout = 180,
    )

    # Raise error
    response.raise_for_status()

    # Convert JSON to overpy object
    results = overpy.Overpass.parse_json(response.text)

    # Iterate over nodes to pull station name, latitude, longitude
    stations = []
    for node in results.nodes:
        name = node.tags.get("name")
        latitude = node.lat
        longitude = node.lon
        if name:
            stations.append({
                "name": name,
                "latitude": latitude,
                "longitude": longitude,
                "geography": f"POINT({longitude} {latitude})"
            })
    
    # Generate dataframe for results
    df = pd.DataFrame(stations)

    # Return df
    return df