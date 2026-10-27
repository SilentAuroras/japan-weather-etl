import os
import overpy
import pandas as pd
import requests

# Define headers
OVERPASS_USER_AGENT = os.getenv("OVERPASS_USER_AGENT")
OVERPASS_REFERER = os.getenv("OVERPASS_REFERER")

# Generate a list of train stations in Japan with their latitude and longitude
def generate_stations_list():

    # Query rail stations in Japan using ISO 3166-1 code for Japan (JP)
    # Focus on shinkansen to limit API queries
    query = '''
    [out:json][timeout:60];
    nwr
    ["railway"="station"]
    ["highspeed"="yes"]
    (area:3600382313);
    out center tags qt;
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
        timeout = 60,
    )

    # Raise error
    response.raise_for_status()

    # Convert JSON to overpy object
    results = overpy.Overpass().parse_json(response.text)

    # Built stations list
    stations = []

    # Nodes plus ways/relations (which carry a center point)
    for element in results.nodes + results.ways + results.relations:

        # Skip those without name
        name = element.tags.get("name")
        if not name:
            continue

        # Pull lat/long from elements
        latitude = getattr(element, "lat", getattr(element, "center_lat", None))
        longitude = getattr(element, "lon", getattr(element, "center_lon", None))

        # Skip elements with empty lat/long
        if latitude is None or longitude is None:
            continue

        # Build stations record
        stations.append({

            # Standard columns
            "name": name,
            "latitude": latitude,
            "longitude": longitude,

            # BigQuery geography column
            "geography": f"POINT({longitude} {latitude})",
        })

    # Generate dataframe for results
    df = pd.DataFrame(stations)

    # Return df
    return df