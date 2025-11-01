# Earthquake data API calls

# Pull earthquake data from the Japan Meteorological Agency
#   - List of recent quakes - https://www.jma.go.jp/bosai/quake/data/list.json
#   - Drill down into each event's json to get each event's location including: latitude and longitude
#       - Example: https://www.jma.go.jp/bosai/quake/data/20250903060513_20250903060214_VXSE5k_1.json

import requests
import pandas as pd
import uuid

def get_earthquake_events():
    """
    Retrieve earthquake events from Earthquake API.
    :return: random_uuid
    """

    # Request the list of recent quake events
    df_recent_events = pd.DataFrame()
    recent_data_url = "https://www.jma.go.jp/bosai/quake/data/list.json"
    
    # Send request
    response = requests.get(recent_data_url)
    if response.status_code == 200:
        recent_events_json = response.json()
        df_recent_events = pd.DataFrame(recent_events_json)

    # Parse list of events
    events_list = df_recent_events["json"]

    # DataFrame for all detailed reports
    df_detailed_events = pd.DataFrame()                         
    
    # Request JSON details for each quake event
    for event in events_list:
    
        # Generate url for each event
        url = "https://www.jma.go.jp/bosai/quake/data/" + event
        # print('retrieving: ' + url)
        
        # Send request
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            temp_df = pd.json_normalize(response_json)
            df_detailed_events = pd.concat([df_detailed_events, temp_df], ignore_index=True)

    # Check if dataframe is empty
    if not df_detailed_events.empty:
    
        # Generate one single parquet file for this pull of quake data
        # Generate random UUID for parquet storage
        random_uuid = uuid.uuid4()

        # Drop problematic columns before saving to parquet - serialization issue
        for col in ["Head.Headline.Information", "Head.Headline.Information.Item"]:
            if col in df_detailed_events.columns:
                df_detailed_events = df_detailed_events.drop([col], axis=1)

        # Create a parquet file locally
        filename = f"data/raw/quake_{random_uuid}.parquet"
        df_detailed_events.to_parquet(filename)

        # Return UUID for tracking
        return random_uuid

    # Dataframe is empty, do not create parquet file, return 1 error
    else:
        return 1